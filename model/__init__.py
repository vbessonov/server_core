# encoding: utf-8
from cStringIO import StringIO
from collections import (
    Counter,
    defaultdict,
)
from lxml import etree
from nose.tools import set_trace
import base64
import bisect
import datetime
import isbnlib
import json
import logging
import md5
import operator
import os
import random
import re
import requests
from threading import RLock
import time
import traceback
import urllib
import urlparse
import uuid
import warnings
import bcrypt

from PIL import (
    Image,
)

from psycopg2.extras import NumericRange
from sqlalchemy.engine.base import Connection
from sqlalchemy import exc as sa_exc
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import (
    event,
    exists,
    func,
    MetaData,
    Table,
    text,
)
from sqlalchemy.sql import select
from sqlalchemy.orm import (
    backref,
    contains_eager,
    joinedload,
    lazyload,
    mapper,
    relationship,
    sessionmaker,
    synonym,
)
from sqlalchemy.orm.base import NO_VALUE
from sqlalchemy.orm.exc import (
    NoResultFound,
    MultipleResultsFound,
)
from sqlalchemy.ext.mutable import (
    MutableDict,
)
from sqlalchemy.ext.associationproxy import (
    association_proxy,
)
from sqlalchemy.ext.hybrid import (
    hybrid_property,
)
from sqlalchemy.sql.functions import func
from sqlalchemy.sql.expression import (
    cast,
    and_,
    or_,
    select,
    join,
    literal,
    literal_column,
    case,
    table,
)
from sqlalchemy.exc import (
    IntegrityError
)
from sqlalchemy import (
    create_engine,
    func,
    Binary,
    Boolean,
    Column,
    Date,
    DateTime,
    Enum,
    Float,
    ForeignKey,
    Index,
    Integer,
    Numeric,
    String,
    Table,
    Unicode,
    UniqueConstraint,
)

import log # Make sure logging is set up properly.
from config import (
    Configuration,
    CannotLoadConfiguration,
)
import classifier
from classifier import (
    Classifier,
    Erotica,
    COMICS_AND_GRAPHIC_NOVELS,
    GenreData,
    WorkClassifier,
)
from entrypoint import EntryPoint
from facets import FacetConstants
from user_profile import ProfileStorage
from util import (
    fast_query_count,
    LanguageCodes,
    MetadataSimilarity,
    TitleProcessor,
)

from mirror import MirrorUploader
from util.http import (
    HTTP,
    RemoteIntegrationException,
)
from util.permanent_work_id import WorkIDCalculator
from util.personal_names import display_name_to_sort_name
from util.summary import SummaryEvaluator

from sqlalchemy.orm.session import Session

from sqlalchemy.dialects.postgresql import (
    ARRAY,
    HSTORE,
    JSON,
    INT4RANGE,
)

DEBUG = False

def production_session():
    url = Configuration.database_url()
    if url.startswith('"'):
        url = url[1:]
    logging.debug("Database url: %s", url)
    _db = SessionManager.session(url)

    # The first thing to do after getting a database connection is to
    # set up the logging configuration.
    #
    # If called during a unit test, this will configure logging
    # incorrectly, but 1) this method isn't normally called during
    # unit tests, and 2) package_setup() will call initialize() again
    # with the right arguments.
    from log import LogConfiguration
    LogConfiguration.initialize(_db)
    return _db

class PolicyException(Exception):
    pass

class BaseMaterializedWork(object):
    """A mixin class for materialized views that incorporate Work and Edition."""
    pass


class HasFullTableCache(object):
    """A mixin class for ORM classes that maintain an in-memory cache of
    (hopefully) every item in the database table for performance reasons.
    """

    RESET = object()

    # You MUST define your own class-specific '_cache' and '_id_cache'
    # variables, like so:
    #
    # _cache = HasFullTableCache.RESET
    # _id_cache = HasFullTableCache.RESET

    @classmethod
    def reset_cache(cls):
        cls._cache = cls.RESET
        cls._id_cache = cls.RESET

    def cache_key(self):
        raise NotImplementedError()

    @classmethod
    def _cache_insert(cls, obj, cache, id_cache):
        """Cache an object for later retrieval, possibly by a different
        database session.
        """
        key = obj.cache_key()
        id = obj.id
        try:
            if cache != cls.RESET:
                cache[key] = obj
            if id_cache != cls.RESET:
                id_cache[id] = obj
        except TypeError, e:
            # The cache was reset in between the time we checked for a
            # reset and the time we tried to put an object in the
            # cache. Stop trying to mess with the cache.
            pass

    @classmethod
    def populate_cache(cls, _db):
        """Populate the in-memory caches from scratch with every single
        object from the database table.
        """
        cache = {}
        id_cache = {}
        for obj in _db.query(cls):
            cls._cache_insert(obj, cache, id_cache)
        cls._cache = cache
        cls._id_cache = id_cache

    @classmethod
    def _cache_lookup(cls, _db, cache, cache_name, cache_key, lookup_hook):
        """Helper method used by both by_id and by_cache_key.

        Looks up `cache_key` in `cache` and calls `lookup_hook`
        to find/create it if it's not in there.
        """
        new = False
        obj = None
        if cache == cls.RESET:
            # The cache has been reset. Populate it with the contents
            # of the table.
            cls.populate_cache(_db)

            # Get the new value of the cache, replacing the value
            # that turned out to be cls.RESET.
            cache = getattr(cls, cache_name)

        if cache != cls.RESET:
            try:
                obj = cache.get(cache_key)
            except TypeError, e:
                # This shouldn't happen. Even if the actual cache was
                # reset just now, we still have a copy of the 'old'
                # cache which passed the 'cache != cls.RESET' test.
                pass

        if not obj:
            # Either this object didn't exist when the cache was
            # populated, or the cache was reset while we were trying
            # to look it up.
            #
            # Give up on the cache and go direct to the database,
            # creating the object if necessary.
            if lookup_hook:
                obj, new = lookup_hook()
            else:
                obj = None
            if not obj:
                # The object doesn't exist and couldn't be created.
                return obj, new

            # Stick the object in the caches, assuming they're not
            # currently in a reset state.
            cls._cache_insert(obj, cls._cache, cls._id_cache)

        if obj and obj not in _db:
            try:
                obj = _db.merge(obj, load=False)
            except Exception, e:
                logging.error(
                    "Unable to merge cached object %r into database session",
                    obj, exc_info=e
                )
                # Try to look up a fresh copy of the object.
                obj, new = lookup_hook()
                if obj and obj in _db:
                    logging.error("Was able to look up a fresh copy of %r", obj)
                    return obj, new

                # That didn't work. Re-raise the original exception.
                logging.error("Unable to look up a fresh copy of %r", obj)
                raise e
        return obj, new

    @classmethod
    def by_id(cls, _db, id):
        """Look up an item by its unique database ID."""
        def lookup_hook():
            return get_one(_db, cls, id=id), False
        obj, is_new = cls._cache_lookup(
            _db, cls._id_cache, '_id_cache', id, lookup_hook
        )
        return obj

    @classmethod
    def by_cache_key(cls, _db, cache_key, lookup_hook):
        return cls._cache_lookup(
            _db, cls._cache, '_cache', cache_key, lookup_hook
        )

def get_one(db, model, on_multiple='error', constraint=None, **kwargs):
    """Gets an object from the database based on its attributes.

    :param constraint: A single clause that can be passed into
        `sqlalchemy.Query.filter` to limit the object that is returned.
    :return: object or None
    """
    constraint = constraint
    if 'constraint' in kwargs:
        constraint = kwargs['constraint']
        del kwargs['constraint']

    q = db.query(model).filter_by(**kwargs)
    if constraint is not None:
        q = q.filter(constraint)

    try:
        return q.one()
    except MultipleResultsFound, e:
        if on_multiple == 'error':
            raise e
        elif on_multiple == 'interchangeable':
            # These records are interchangeable so we can use
            # whichever one we want.
            #
            # This may be a sign of a problem somewhere else. A
            # database-level constraint might be useful.
            q = q.limit(1)
            return q.one()
    except NoResultFound:
        return None

def get_one_or_create(db, model, create_method='',
                      create_method_kwargs=None,
                      **kwargs):
    one = get_one(db, model, **kwargs)
    if one:
        return one, False
    else:
        __transaction = db.begin_nested()
        try:
            # These kwargs are supported by get_one() but not by create().
            get_one_keys = ['on_multiple', 'constraint']
            for key in get_one_keys:
                if key in kwargs:
                    del kwargs[key]
            obj = create(db, model, create_method, create_method_kwargs, **kwargs)
            __transaction.commit()
            return obj
        except IntegrityError, e:
            logging.info(
                "INTEGRITY ERROR on %r %r, %r: %r", model, create_method_kwargs,
                kwargs, e)
            __transaction.rollback()
            return db.query(model).filter_by(**kwargs).one(), False

def flush(db):
    """Flush the database connection unless it's known to already be flushing."""
    is_flushing = False
    if hasattr(db, '_flushing'):
        # This is a regular database session.
        is_flushing = db._flushing
    elif hasattr(db, 'registry'):
        # This is a flask_scoped_session scoped session.
        is_flushing = db.registry()._flushing
    else:
        logging.error("Unknown database connection type: %r", db)
    if not is_flushing:
        db.flush()

def create(db, model, create_method='',
           create_method_kwargs=None,
           **kwargs):
    kwargs.update(create_method_kwargs or {})
    created = getattr(model, create_method, model)(**kwargs)
    db.add(created)
    flush(db)
    return created, True

class MediaTypeConstants(object):

    EPUB_MEDIA_TYPE = u"application/epub+zip"
    PDF_MEDIA_TYPE = u"application/pdf"
    MOBI_MEDIA_TYPE = u"application/x-mobipocket-ebook"
    AMAZON_KF8_MEDIA_TYPE = u"application/x-mobi8-ebook"
    TEXT_XML_MEDIA_TYPE = u"text/xml"
    TEXT_HTML_MEDIA_TYPE = u"text/html"
    APPLICATION_XML_MEDIA_TYPE = u"application/xml"
    JPEG_MEDIA_TYPE = u"image/jpeg"
    PNG_MEDIA_TYPE = u"image/png"
    GIF_MEDIA_TYPE = u"image/gif"
    SVG_MEDIA_TYPE = u"image/svg+xml"
    MP3_MEDIA_TYPE = u"audio/mpeg"
    MP4_MEDIA_TYPE = u"video/mp4"
    WMV_MEDIA_TYPE = u"video/x-ms-wmv"
    SCORM_MEDIA_TYPE = u"application/vnd.librarysimplified.scorm+zip"
    ZIP_MEDIA_TYPE = u"application/zip"
    OCTET_STREAM_MEDIA_TYPE = u"application/octet-stream"
    TEXT_PLAIN = u"text/plain"
    AUDIOBOOK_MANIFEST_MEDIA_TYPE = u"application/audiobook+json"

    BOOK_MEDIA_TYPES = [
        EPUB_MEDIA_TYPE,
        PDF_MEDIA_TYPE,
        MOBI_MEDIA_TYPE,
        MP3_MEDIA_TYPE,
        AMAZON_KF8_MEDIA_TYPE,
    ]

    # These media types are in the order we would prefer to use them.
    # e.g. all else being equal, we would prefer a PNG to a JPEG.
    IMAGE_MEDIA_TYPES = [
        PNG_MEDIA_TYPE,
        JPEG_MEDIA_TYPE,
        GIF_MEDIA_TYPE,
        SVG_MEDIA_TYPE,
    ]

    SUPPORTED_BOOK_MEDIA_TYPES = [
        EPUB_MEDIA_TYPE
    ]

    # Most of the time, if you believe a resource to be media type A,
    # but then you make a request and get media type B, then the
    # actual media type (B) takes precedence over what you thought it
    # was (A). These media types are the exceptions: they are so
    # generic that they don't tell you anything, so it's more useful
    # to stick with A.
    GENERIC_MEDIA_TYPES = [OCTET_STREAM_MEDIA_TYPE]

    FILE_EXTENSIONS = {
        EPUB_MEDIA_TYPE: "epub",
        MOBI_MEDIA_TYPE: "mobi",
        PDF_MEDIA_TYPE: "pdf",
        MP3_MEDIA_TYPE: "mp3",
        MP4_MEDIA_TYPE: "mp4",
        WMV_MEDIA_TYPE: "wmv",
        JPEG_MEDIA_TYPE: "jpg",
        PNG_MEDIA_TYPE: "png",
        SVG_MEDIA_TYPE: "svg",
        GIF_MEDIA_TYPE: "gif",
        ZIP_MEDIA_TYPE: "zip",
        TEXT_PLAIN: "txt",
        TEXT_HTML_MEDIA_TYPE: "html",
        APPLICATION_XML_MEDIA_TYPE: "xml",
        AUDIOBOOK_MANIFEST_MEDIA_TYPE: "audiobook-manifest",
        SCORM_MEDIA_TYPE: "zip"
    }

    COMMON_EBOOK_EXTENSIONS = ['.epub', '.pdf']
    COMMON_IMAGE_EXTENSIONS = ['.jpg', '.jpeg', '.png', '.gif']

    # Invert FILE_EXTENSIONS and add some extra guesses.
    MEDIA_TYPE_FOR_EXTENSION = {
        ".htm" : TEXT_HTML_MEDIA_TYPE,
        ".jpeg" : JPEG_MEDIA_TYPE,
    }
    for media_type, extension in FILE_EXTENSIONS.items():
        MEDIA_TYPE_FOR_EXTENSION['.' + extension] = media_type

class DataSourceConstants(object):
    GUTENBERG = u"Gutenberg"
    OVERDRIVE = u"Overdrive"
    ODILO = u"Odilo"
    PROJECT_GITENBERG = u"Project GITenberg"
    STANDARD_EBOOKS = u"Standard Ebooks"
    UNGLUE_IT = u"unglue.it"
    BIBLIOTHECA = u"Bibliotheca"
    OCLC = u"OCLC Classify"
    OCLC_LINKED_DATA = u"OCLC Linked Data"
    AMAZON = u"Amazon"
    XID = u"WorldCat xID"
    AXIS_360 = u"Axis 360"
    WEB = u"Web"
    OPEN_LIBRARY = u"Open Library"
    CONTENT_CAFE = u"Content Cafe"
    VIAF = u"VIAF"
    GUTENBERG_COVER_GENERATOR = u"Gutenberg Illustrated"
    GUTENBERG_EPUB_GENERATOR = u"Project Gutenberg EPUB Generator"
    METADATA_WRANGLER = u"Library Simplified metadata wrangler"
    MANUAL = u"Manual intervention"
    NOVELIST = u"NoveList Select"
    NYT = u"New York Times"
    NYPL_SHADOWCAT = u"NYPL Shadowcat"
    LIBRARY_STAFF = u"Library staff"
    ADOBE = u"Adobe DRM"
    PLYMPTON = u"Plympton"
    RB_DIGITAL = u"RBdigital"
    ELIB = u"eLiburutegia"
    OA_CONTENT_SERVER = u"Library Simplified Open Access Content Server"
    PRESENTATION_EDITION = u"Presentation edition generator"
    INTERNAL_PROCESSING = u"Library Simplified Internal Process"
    FEEDBOOKS = u"FeedBooks"
    BIBBLIO = u"Bibblio"
    ENKI = u"Enki"

    DEPRECATED_NAMES = {
        u"3M" : BIBLIOTHECA,
        u"OneClick" : RB_DIGITAL,
    }
    THREEM = BIBLIOTHECA
    ONECLICK = RB_DIGITAL

    # Some sources of open-access ebooks are better than others. This
    # list shows which sources we prefer, in ascending order of
    # priority. unglue.it is lowest priority because it tends to
    # aggregate books from other sources. We prefer books from their
    # original sources.
    OPEN_ACCESS_SOURCE_PRIORITY = [
        UNGLUE_IT,
        GUTENBERG,
        GUTENBERG_EPUB_GENERATOR,
        PROJECT_GITENBERG,
        ELIB,
        FEEDBOOKS,
        PLYMPTON,
        STANDARD_EBOOKS,
    ]

    # When we're generating the presentation edition for a
    # LicensePool, editions are processed based on their data source,
    # in the following order:
    #
    # [all other sources] < [source of the license pool] < [metadata
    # wrangler] < [library staff] < [manual intervention]
    #
    # This list keeps track of the high-priority portion of that
    # ordering.
    #
    # "LIBRARY_STAFF" comes from the Admin Interface.
    # "MANUAL" is not currently used, but will give the option of putting in
    # software engineer-created system overrides.
    PRESENTATION_EDITION_PRIORITY = [METADATA_WRANGLER, LIBRARY_STAFF, MANUAL]

Base = declarative_base()

from bibliographic_metadata import (
    DataSource,
    Identifier,
    Equivalency,
    Edition,
    Contributor,
    Contribution,
    WorkContribution,
)
from background import (
    BaseCoverageRecord,
    Timestamp,
    CoverageRecord,
    WorkCoverageRecord,
)
from cached_feed import (
    CachedFeed,
    WillNotGenerateExpensiveFeed,
)
from circulation_event import (
    CirculationEvent,
)
from classification import (
    Classification,
    Subject,
    Genre,
)
from collection import (
    Collection,
)
from configuration import (
    ExternalIntegration,
    ConfigurationSetting,
    Admin,
    AdminRole
)
from credentials import (
    Credential,
    DRMDeviceIdentifier,
    DelegatedPatronIdentifier,
)
from custom_lists import (
    CustomList,
    CustomListEntry,
)
from library import (
    Library,
)
from licensing import (
    Collection,
    LicensePool,
    LicensePoolDeliveryMechanism,
    DeliveryMechanism,
    RightsStatus,
    Complaint,
    IntegrationClient,
    CollectionMissing,
)
from measurement import (
    Measurement,
)
from patrons import (
    Patron,
    Loan,
    Hold,
    Annotation,
    PatronProfileStorage,
    LoanAndHoldMixin,
)
from resources import (
    Resource,
    ResourceTransformation,
    Hyperlink,
    Representation
)
from works import (
    Work,
    WorkGenre,
    PresentationCalculationPolicy,
    SessionManager,
)
# Most of the time, we can know whether a change to the database is
# likely to require that the application reload the portion of the
# configuration it gets from the database. These hooks will call
# site_configuration_has_changed() whenever such a change happens.
#
# This is not supposed to be a comprehensive list of changes that
# should trigger a ConfigurationSetting reload -- that needs to be
# handled on the application level -- but it should be good enough to
# catch most that slip through the cracks.
@event.listens_for(Collection.children, 'append')
@event.listens_for(Collection.children, 'remove')
@event.listens_for(Collection.libraries, 'append')
@event.listens_for(Collection.libraries, 'remove')
@event.listens_for(ExternalIntegration.settings, 'append')
@event.listens_for(ExternalIntegration.settings, 'remove')
@event.listens_for(Library.integrations, 'append')
@event.listens_for(Library.integrations, 'remove')
@event.listens_for(Library.settings, 'append')
@event.listens_for(Library.settings, 'remove')
def configuration_relevant_collection_change(target, value, initiator):
    site_configuration_has_changed(target)

@event.listens_for(Library, 'after_insert')
@event.listens_for(Library, 'after_delete')
@event.listens_for(ExternalIntegration, 'after_insert')
@event.listens_for(ExternalIntegration, 'after_delete')
@event.listens_for(Collection, 'after_insert')
@event.listens_for(Collection, 'after_delete')
@event.listens_for(ConfigurationSetting, 'after_insert')
@event.listens_for(ConfigurationSetting, 'after_delete')
def configuration_relevant_lifecycle_event(mapper, connection, target):
    site_configuration_has_changed(target)

@event.listens_for(Library, 'after_update')
@event.listens_for(ExternalIntegration, 'after_update')
@event.listens_for(Collection, 'after_update')
@event.listens_for(ConfigurationSetting, 'after_update')
def configuration_relevant_update(mapper, connection, target):
    if directly_modified(target):
        site_configuration_has_changed(target)

@event.listens_for(Admin, 'after_insert')
@event.listens_for(Admin, 'after_delete')
@event.listens_for(Admin, 'after_update')
def refresh_admin_cache(mapper, connection, target):
    # The next time someone tries to access an Admin,
    # the cache will be repopulated.
    Admin.reset_cache()

@event.listens_for(AdminRole, 'after_insert')
@event.listens_for(AdminRole, 'after_delete')
@event.listens_for(AdminRole, 'after_update')
def refresh_admin_role_cache(mapper, connection, target):
    # The next time someone tries to access an AdminRole,
    # the cache will be repopulated.
    AdminRole.reset_cache()

@event.listens_for(Collection, 'after_insert')
@event.listens_for(Collection, 'after_delete')
@event.listens_for(Collection, 'after_update')
def refresh_collection_cache(mapper, connection, target):
    # The next time someone tries to access a Collection,
    # the cache will be repopulated.
    Collection.reset_cache()

@event.listens_for(ConfigurationSetting, 'after_insert')
@event.listens_for(ConfigurationSetting, 'after_delete')
@event.listens_for(ConfigurationSetting, 'after_update')
def refresh_configuration_settings(mapper, connection, target):
    # The next time someone tries to access a configuration setting,
    # the cache will be repopulated.
    ConfigurationSetting.reset_cache()

@event.listens_for(DataSource, 'after_insert')
@event.listens_for(DataSource, 'after_delete')
@event.listens_for(DataSource, 'after_update')
def refresh_datasource_cache(mapper, connection, target):
    # The next time someone tries to access a DataSource,
    # the cache will be repopulated.
    DataSource.reset_cache()

@event.listens_for(DeliveryMechanism, 'after_insert')
@event.listens_for(DeliveryMechanism, 'after_delete')
@event.listens_for(DeliveryMechanism, 'after_update')
def refresh_datasource_cache(mapper, connection, target):
    # The next time someone tries to access a DeliveryMechanism,
    # the cache will be repopulated.
    DeliveryMechanism.reset_cache()

@event.listens_for(ExternalIntegration, 'after_insert')
@event.listens_for(ExternalIntegration, 'after_delete')
@event.listens_for(ExternalIntegration, 'after_update')
def refresh_datasource_cache(mapper, connection, target):
    # The next time someone tries to access an ExternalIntegration,
    # the cache will be repopulated.
    ExternalIntegration.reset_cache()

@event.listens_for(Library, 'after_insert')
@event.listens_for(Library, 'after_delete')
@event.listens_for(Library, 'after_update')

def refresh_library_cache(mapper, connection, target):
    # The next time someone tries to access a library,
    # the cache will be repopulated.
    Library.reset_cache()

@event.listens_for(Genre, 'after_insert')
@event.listens_for(Genre, 'after_delete')
@event.listens_for(Genre, 'after_update')

def refresh_genre_cache(mapper, connection, target):
    # The next time someone tries to access a genre,
    # the cache will be repopulated.
    #
    # The only time this should really happen is the very first time a
    # site is brought up, but just in case.
    Genre.reset_cache()

@event.listens_for(LicensePool.work_id, 'set')
@event.listens_for(Work.presentation_edition_id, 'set')
# When a pool gets a work and a presentation edition for the first time,
# the work should be added to any custom lists associated with the pool's
# collection.
# In some cases, the work may be generated before the presentation edition.
# Then we need to add it when the work gets a presentation edition.
def add_work_to_customlists_for_collection(pool_or_work, value, oldvalue, initiator):
    if isinstance(pool_or_work, LicensePool):
        work = pool_or_work.work
        pools = [pool_or_work]
    else:
        work = pool_or_work
        pools = work.license_pools

    if (not oldvalue or oldvalue is NO_VALUE) and value and work and work.presentation_edition:
        for pool in pools:
            if not pool.collection:
                # This shouldn't happen, but don't crash if it does --
                # the correct behavior is that the work not be added to
                # any CustomLists.
                continue
            for list in pool.collection.customlists:
                # Since the work was just created, we can assume that
                # there's already a pending registration for updating the
                # work's internal index, and decide not to create a
                # second one.
                list.add_entry(work, featured=True, update_external_index=False)
