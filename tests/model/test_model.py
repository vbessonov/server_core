# encoding: utf-8
from StringIO import StringIO
import base64
import datetime
import feedparser
import os
import sys
import site
import random
import re
import tempfile

from nose.tools import (
    assert_raises,
    assert_raises_regexp,
    assert_not_equal,
    eq_,
    set_trace,
)

from psycopg2.extras import NumericRange

from sqlalchemy import not_

from sqlalchemy.exc import (
    IntegrityError,
)

from sqlalchemy.orm.exc import (
    NoResultFound,
    MultipleResultsFound
)
from sqlalchemy.orm.session import Session

from lxml import etree

from core.config import (
    CannotLoadConfiguration,
    Configuration,
    temp_config,
)

from core.entrypoint import AudiobooksEntryPoint

import core.lane
from core.lane import (
    Facets,
    Pagination,
    WorkList,
)
import core.model
from core.model import (
    Admin,
    AdminRole,
    Annotation,
    BaseCoverageRecord,
    CachedFeed,
    CirculationEvent,
    Classification,
    Collection,
    CollectionMissing,
    Complaint,
    ConfigurationSetting,
    Contributor,
    CoverageRecord,
    Credential,
    CustomList,
    CustomListEntry,
    DataSource,
    DelegatedPatronIdentifier,
    DeliveryMechanism,
    DRMDeviceIdentifier,
    ExternalIntegration,
    Genre,
    HasFullTableCache,
    Hold,
    Hyperlink,
    IntegrationClient,
    Library,
    LicensePool,
    LicensePoolDeliveryMechanism,
    Measurement,
    Patron,
    PatronProfileStorage,
    PolicyException,
    Representation,
    Resource,
    RightsStatus,
    SessionManager,
    Subject,
    Timestamp,
    Work,
    WorkCoverageRecord,
    WorkGenre,
    Identifier,
    Edition,
    create,
    get_one,
    get_one_or_create,
    site_configuration_has_changed,
    tuple_to_numericrange,
)
from core.external_search import (
    DummyExternalSearchIndex,
)

import core.classifier
from core.classifier import (
    Classifier,
    Fantasy,
    Romance,
    Science_Fiction,
    Drama,
)

from .. import (
    DatabaseTest,
    DummyHTTPClient,
)

from testing import MockRequestsResponse

from mock_analytics_provider import MockAnalyticsProvider


class TestDatabaseInterface(DatabaseTest):

    def test_get_one(self):

        # When a matching object isn't found, None is returned.
        result = get_one(self._db, Edition)
        eq_(None, result)

        # When a single item is found, it is returned.
        edition = self._edition()
        result = get_one(self._db, Edition)
        eq_(edition, result)

        # When multiple items are found, an error is raised.
        other_edition = self._edition()
        assert_raises(MultipleResultsFound, get_one, self._db, Edition)

        # Unless they're interchangeable.
        result = get_one(self._db, Edition, on_multiple='interchangeable')
        assert result in self._db.query(Edition)

        # Or specific attributes are passed that limit the results to one.
        result = get_one(
            self._db, Edition,
            title=other_edition.title,
            author=other_edition.author)
        eq_(other_edition, result)

        # A particular constraint clause can also be passed in.
        titles = [ed.title for ed in (edition, other_edition)]
        constraint = not_(Edition.title.in_(titles))
        result = get_one(self._db, Edition, constraint=constraint)
        eq_(None, result)

    def test_initialize_data_does_not_reset_timestamp(self):
        # initialize_data() has already been called, so the database is
        # initialized and the 'site configuration changed' Timestamp has
        # been set. Calling initialize_data() again won't change the
        # date on the timestamp.
        timestamp = get_one(self._db, Timestamp,
                            collection=None,
                            service=Configuration.SITE_CONFIGURATION_CHANGED)
        old_timestamp = timestamp.timestamp
        SessionManager.initialize_data(self._db)
        eq_(old_timestamp, timestamp.timestamp)












# class TestWorkQuality(DatabaseTest):

#     def test_better_known_work_gets_higher_rating(self):

#         gutenberg_source = DataSource.lookup(self._db, DataSource.GUTENBERG)

#         edition1_1, pool1 = self._edition(with_license_pool=True)
#         edition1_2 = self._edition(with_license_pool=False)

#         edition2_1, pool2 = self._edition(with_license_pool=True)

#         wrs = []
#         pools = []
#         for i in range(10):
#             wr, pool = self._edition(with_license_pool=True)
#             wrs.append(wr)
#             pools.append(pool)

#         work1 = Work()
#         work1.editions.extend([edition1_1, edition1_2] + wrs)
#         work1.license_pools.extend(pools + [pool1])

#         work2 = Work()
#         work2.editions.append(edition2_1)
#         work2.license_pools.append(pool2)

#         work1.calculate_presentation()
#         work2.calculate_presentation()

#         assert work1.quality > work2.quality

#     def test_more_license_pools_gets_higher_rating(self):

#         gutenberg_source = DataSource.lookup(self._db, DataSource.GUTENBERG)

#         edition1_1, pool1 = self._edition(with_license_pool=True)
#         edition1_2, pool2 = self._edition(with_license_pool=True)

#         edition2_1, pool3 = self._edition(with_license_pool=True)
#         edition2_2 = self._edition(with_license_pool=False)

#         wrs = []
#         pools = []
#         for i in range(10):
#             wr, pool = self._edition(with_license_pool=True)
#             wrs.append(wr)
#             pools.append(pool)

#         work1 = Work()
#         work1.editions.extend([edition1_1, edition1_2] + wrs)
#         work1.license_pools.extend([pool1, pool2] + pools)

#         work2 = Work()
#         work2.editions.extend([edition2_1, edition2_2])
#         work2.license_pools.extend([pool3])

#         work1.calculate_presentation()
#         work2.calculate_presentation()

#         assert work1.quality > work2.quality


class TestWorkConsolidation(DatabaseTest):

    def test_calculate_work_success(self):
        e, p = self._edition(with_license_pool=True)
        work, new = p.calculate_work()
        eq_(p.presentation_edition, work.presentation_edition)
        eq_(True, new)

    def test_calculate_work_bails_out_if_no_title(self):
        e, p = self._edition(with_license_pool=True)
        e.title=None
        work, new = p.calculate_work()
        eq_(None, work)
        eq_(False, new)

        # even_if_no_title means we don't need a title.
        work, new = p.calculate_work(even_if_no_title=True)
        assert isinstance(work, Work)
        eq_(True, new)
        eq_(None, work.title)
        eq_(None, work.presentation_edition.permanent_work_id)

    def test_calculate_work_even_if_no_author(self):
        title = "Book"
        e, p = self._edition(with_license_pool=True, authors=[], title=title)
        work, new = p.calculate_work()
        eq_(title, work.title)
        eq_(True, new)

    def test_calculate_work_matches_based_on_permanent_work_id(self):
        # Here are two Editions with the same permanent work ID,
        # since they have the same title/author.
        edition1, ignore = self._edition(with_license_pool=True)
        edition2, ignore = self._edition(
            title=edition1.title, authors=edition1.author,
            with_license_pool=True
        )

        # For purposes of this test, let's pretend all these books are
        # open-access.
        for e in [edition1, edition2]:
            for license_pool in e.license_pools:
                license_pool.open_access = True

        # Calling calculate_work() on the first edition creates a Work.
        work1, created = edition1.license_pools[0].calculate_work()
        eq_(created, True)

        # Calling calculate_work() on the second edition associated
        # the second edition's pool with the first work.
        work2, created = edition2.license_pools[0].calculate_work()
        eq_(created, False)

        eq_(work1, work2)

        expect = edition1.license_pools + edition2.license_pools
        eq_(set(expect), set(work1.license_pools))


    def test_calculate_work_for_licensepool_creates_new_work(self):
        edition1, ignore = self._edition(data_source_name=DataSource.GUTENBERG, identifier_type=Identifier.GUTENBERG_ID,
            title=self._str, authors=[self._str], with_license_pool=True)

        # This edition is unique to the existing work.
        preexisting_work = Work()
        preexisting_work.set_presentation_edition(edition1)

        # This edition is unique to the new LicensePool
        edition2, pool = self._edition(data_source_name=DataSource.GUTENBERG, identifier_type=Identifier.GUTENBERG_ID,
            title=self._str, authors=[self._str], with_license_pool=True)

        # Call calculate_work(), and a new Work is created.
        work, created = pool.calculate_work()
        eq_(True, created)
        assert work != preexisting_work

    def test_calculate_work_does_nothing_unless_edition_has_title(self):
        collection=self._collection()
        edition, ignore = Edition.for_foreign_id(
            self._db, DataSource.GUTENBERG, Identifier.GUTENBERG_ID, "1",
        )
        pool, ignore = LicensePool.for_foreign_id(
            self._db, DataSource.GUTENBERG, Identifier.GUTENBERG_ID, "1",
            collection=collection
        )
        work, created = pool.calculate_work()
        eq_(None, work)

        edition.title = u"foo"
        work, created = pool.calculate_work()
        edition.calculate_presentation()
        eq_(True, created)
        #
        # # The edition is the work's presentation edition.
        eq_(work, edition.work)
        eq_(edition, work.presentation_edition)
        eq_(u"foo", work.title)
        eq_(u"[Unknown]", work.author)

    def test_calculate_work_fails_when_presentation_edition_identifier_does_not_match_license_pool(self):

        # Here's a LicensePool with an Edition.
        edition1, pool = self._edition(
            data_source_name=DataSource.GUTENBERG, with_license_pool=True
        )

        # Here's a second Edition that's talking about a different Identifier
        # altogether, and has no LicensePool.
        edition2 = self._edition()
        assert edition1.primary_identifier != edition2.primary_identifier

        # Here's a third Edition that's tied to a totally different
        # LicensePool.
        edition3, pool2 = self._edition(with_license_pool=True)
        assert edition1.primary_identifier != edition3.primary_identifier

        # When we calculate a Work for a LicensePool, we can pass in
        # any Edition as the presentation edition, so long as that
        # Edition's primary identifier matches the LicensePool's
        # identifier.
        work, is_new = pool.calculate_work(known_edition=edition1)

        # But we can't pass in an Edition that's the presentation
        # edition for a LicensePool with a totally different Identifier.
        for edition in (edition2, edition3):
            assert_raises_regexp(
                ValueError,
                "Alleged presentation edition is not the presentation edition for the license pool for which work is being calculated!",
                pool.calculate_work,
                known_edition=edition
            )

    def test_open_access_pools_grouped_together(self):

        # We have four editions with exactly the same title and author.
        # Two of them are open-access, two are not.
        title = "The Only Title"
        author = "Single Author"
        ed1, open1 = self._edition(title=title, authors=author, with_license_pool=True)
        ed2, open2 = self._edition(title=title, authors=author, with_license_pool=True)
        open1.open_access = True
        open2.open_access = True
        ed3, restricted3 = self._edition(
            title=title, authors=author, data_source_name=DataSource.OVERDRIVE,
            with_license_pool=True)
        ed4, restricted4 = self._edition(
            title=title, authors=author, data_source_name=DataSource.OVERDRIVE,
            with_license_pool=True)

        restricted3.open_access = False
        restricted4.open_access = False

        # Every identifier is equivalent to every other identifier.
        s = DataSource.lookup(self._db, DataSource.OCLC_LINKED_DATA)
        ed1.primary_identifier.equivalent_to(s, ed2.primary_identifier, 1)
        ed1.primary_identifier.equivalent_to(s, ed3.primary_identifier, 1)
        ed1.primary_identifier.equivalent_to(s, ed4.primary_identifier, 1)
        ed2.primary_identifier.equivalent_to(s, ed3.primary_identifier, 1)
        ed2.primary_identifier.equivalent_to(s, ed4.primary_identifier, 1)
        ed3.primary_identifier.equivalent_to(s, ed4.primary_identifier, 1)

        open1.calculate_work()
        open2.calculate_work()
        restricted3.calculate_work()
        restricted4.calculate_work()

        assert open1.work != None
        assert open2.work != None
        assert restricted3.work != None
        assert restricted4.work != None

        # The two open-access pools are grouped together.
        eq_(open1.work, open2.work)

        # Each restricted-access pool is completely isolated.
        assert restricted3.work != restricted4.work
        assert restricted3.work != open1.work

    def test_all_licensepools_with_same_identifier_get_same_work(self):

        # Here are two LicensePools for the same Identifier and
        # DataSource, but different Collections.
        edition1, pool1 = self._edition(with_license_pool=True)
        identifier = pool1.identifier
        collection2 = self._collection()

        edition2, pool2 = self._edition(
            with_license_pool=True,
            identifier_type=identifier.type,
            identifier_id=identifier.identifier,
            collection=collection2
        )

        eq_(pool1.identifier, pool2.identifier)
        eq_(pool1.data_source, pool2.data_source)
        eq_(self._default_collection, pool1.collection)
        eq_(collection2, pool2.collection)

        # The two LicensePools have the same Edition (since a given
        # DataSource has only one opinion about an Identifier's
        # bibliographic information).
        eq_(edition1, edition2)

        # Because the two LicensePools have the same Identifier, they
        # have the same Work.
        work1, is_new_1 = pool1.calculate_work()
        work2, is_new_2 = pool2.calculate_work()
        eq_(work1, work2)
        eq_(True, is_new_1)
        eq_(False, is_new_2)
        eq_(edition1, work1.presentation_edition)

    def test_calculate_work_fixes_work_in_invalid_state(self):
        # Here's a Work with a commercial edition of "abcd".
        work = self._work(with_license_pool=True)
        [abcd_commercial] = work.license_pools
        abcd_commercial.open_access = False
        abcd_commercial.presentation_edition.permanent_work_id = "abcd"

        # Due to a earlier error, the Work also contains a _second_
        # commercial edition of "abcd"...
        edition, abcd_commercial_2 = self._edition(with_license_pool=True)
        abcd_commercial_2.open_access = False
        abcd_commercial_2.presentation_edition.permanent_work_id = "abcd"
        work.license_pools.append(abcd_commercial_2)

        # ...as well as an open-access edition of "abcd".
        edition, abcd_open_access = self._edition(
            with_license_pool=True, with_open_access_download=True
        )
        abcd_open_access.open_access = True
        abcd_open_access.presentation_edition.permanent_work_id = "abcd"
        work.license_pools.append(abcd_open_access)

        # calculate_work() recalculates the permanent work ID of a
        # LicensePool's presentation edition, and obviously the real
        # value isn't "abcd" for any of these Editions. Mocking
        # calculate_permanent_work_id ensures that we run the code
        # under the assumption that all these Editions have the same
        # permanent work ID.
        def mock_pwid(debug=False):
            return "abcd"
        for lp in [abcd_commercial, abcd_commercial_2, abcd_open_access]:
            lp.presentation_edition.calculate_permanent_work_id = mock_pwid

        # Anyway, we can fix the whole problem by calling
        # calculate_work() on one of the LicensePools.
        work_after, is_new = abcd_commercial.calculate_work()
        eq_(work_after, work)
        eq_(False, is_new)

        # The LicensePool we called calculate_work() on gets to stay
        # in the Work, but the other two have been kicked out and
        # given their own works.
        assert abcd_commercial_2.work != work
        assert abcd_open_access.work != work

        # The commercial LicensePool has been given a Work of its own.
        eq_([abcd_commercial_2], abcd_commercial_2.work.license_pools)

        # The open-access work has been given the Work that will be
        # used for all open-access LicensePools for that book going
        # forward.

        expect_open_access_work, open_access_work_is_new = (
            Work.open_access_for_permanent_work_id(
                self._db, "abcd", Edition.BOOK_MEDIUM, 'eng'
            )
        )
        eq_(expect_open_access_work, abcd_open_access.work)

        # Now we're going to restore the bad configuration, where all
        # three books have the same Work. This time we're going to
        # call calculate_work() on the open-access LicensePool, and
        # verify that we get similar results as when we call
        # calculate_work() on one of the commercial LicensePools.
        abcd_commercial_2.work = work
        abcd_open_access.work = work

        work_after, is_new = abcd_open_access.calculate_work()
        # Since we called calculate_work() on the open-access work, it
        # maintained control of the Work, and both commercial books
        # got assigned new Works.
        eq_(work, work_after)
        eq_(False, is_new)

        assert abcd_commercial.work != work
        assert abcd_commercial.work != None
        assert abcd_commercial_2.work != work
        assert abcd_commercial_2.work != None
        assert abcd_commercial.work != abcd_commercial_2.work

        # Finally, let's test that nothing happens if you call
        # calculate_work() on a self-consistent situation.
        open_access_work = abcd_open_access.work
        eq_((open_access_work, False), abcd_open_access.calculate_work())

        commercial_work = abcd_commercial.work
        eq_((commercial_work, False), abcd_commercial.calculate_work())

    def test_calculate_work_fixes_incorrectly_grouped_books(self):
        # Here's a Work with an open-access edition of "abcd".
        work = self._work(with_license_pool=True)
        [book] = work.license_pools
        book.open_access = True
        book.presentation_edition.permanent_work_id = "abcd"

        # Due to a earlier error, the Work also contains an
        # open-access _audiobook_ of "abcd".
        edition, audiobook = self._edition(with_license_pool=True)
        audiobook.open_access = True
        audiobook.presentation_edition.medium=Edition.AUDIO_MEDIUM
        audiobook.presentation_edition.permanent_work_id = "abcd"
        work.license_pools.append(audiobook)

        # And the Work _also_ contains an open-access book of "abcd"
        # in a different language.
        edition, spanish = self._edition(with_license_pool=True)
        spanish.open_access = True
        spanish.presentation_edition.language='spa'
        spanish.presentation_edition.permanent_work_id = "abcd"
        work.license_pools.append(spanish)

        def mock_pwid(debug=False):
            return "abcd"
        for lp in [book, audiobook, spanish]:
            lp.presentation_edition.calculate_permanent_work_id = mock_pwid

        # We can fix this by calling calculate_work() on one of the
        # LicensePools.
        work_after, is_new = book.calculate_work()
        eq_(work_after, work)
        eq_(False, is_new)

        # The LicensePool we called calculate_work() on gets to stay
        # in the Work, but the other one has been kicked out and
        # given its own work.
        eq_(book.work, work)
        assert audiobook.work != work

        # The audiobook LicensePool has been given a Work of its own.
        eq_([audiobook], audiobook.work.license_pools)

        # The book has been given the Work that will be used for all
        # book-type LicensePools for that title going forward.
        expect_book_work, book_work_is_new = (
            Work.open_access_for_permanent_work_id(
                self._db, "abcd", Edition.BOOK_MEDIUM, 'eng'
            )
        )
        eq_(expect_book_work, book.work)

        # The audiobook has been given the Work that will be used for
        # all audiobook-type LicensePools for that title going
        # forward.
        expect_audiobook_work, audiobook_work_is_new = (
            Work.open_access_for_permanent_work_id(
                self._db, "abcd", Edition.AUDIO_MEDIUM, 'eng'
            )
        )
        eq_(expect_audiobook_work, audiobook.work)

        # The Spanish book has been given the Work that will be used
        # for all Spanish LicensePools for that title going forward.
        expect_spanish_work, spanish_work_is_new = (
            Work.open_access_for_permanent_work_id(
                self._db, "abcd", Edition.BOOK_MEDIUM, 'spa'
            )
        )
        eq_(expect_spanish_work, spanish.work)
        eq_('spa', expect_spanish_work.language)


    def test_calculate_work_detaches_licensepool_with_no_title(self):
        # Here's a Work with an open-access edition of "abcd".
        work = self._work(with_license_pool=True)
        [book] = work.license_pools
        book.presentation_edition.permanent_work_id = "abcd"

        # But the LicensePool's presentation edition has lost its
        # title.
        book.presentation_edition.title = None

        # Calling calculate_work() on the LicensePool will detach the
        # book from its work, since a book with no title cannot have
        # an associated Work.
        work_after, is_new = book.calculate_work()
        eq_(None, work_after)
        eq_([], work.license_pools)

    def test_calculate_work_detaches_licensepool_with_no_pwid(self):
        # Here's a Work with an open-access edition of "abcd".
        work = self._work(with_license_pool=True)
        [book] = work.license_pools
        book.presentation_edition.permanent_work_id = "abcd"

        # Due to a earlier error, the Work also contains an edition
        # with no title or author, and thus no permanent work ID.
        edition, no_title = self._edition(with_license_pool=True)

        no_title.presentation_edition.title=None
        no_title.presentation_edition.author=None
        no_title.presentation_edition.permanent_work_id = None
        work.license_pools.append(no_title)

        # Calling calculate_work() on the functional LicensePool will
        # split off the bad one.
        work_after, is_new = book.calculate_work()
        eq_([book], work.license_pools)
        eq_(None, no_title.work)
        eq_(None, no_title.presentation_edition.work)

        # calculate_work() on the bad LicensePool will split it off from
        # the good one.
        work.license_pools.append(no_title)
        work_after_2, is_new = no_title.calculate_work()
        eq_(None, work_after_2)
        eq_([book], work.license_pools)

        # The same thing happens if the bad LicensePool has no
        # presentation edition at all.
        work.license_pools.append(no_title)
        no_title.presentation_edition = None
        work_after, is_new = book.calculate_work()
        eq_([book], work.license_pools)

        work.license_pools.append(no_title)
        work_after, is_new = no_title.calculate_work()
        eq_([book], work.license_pools)


    def test_pwids(self):
        """Test the property that finds all permanent work IDs
        associated with a Work.
        """
        # Create a (bad) situation in which LicensePools associated
        # with two different PWIDs are associated with the same work.
        work = self._work(with_license_pool=True)
        [lp1] = work.license_pools
        eq_(set([lp1.presentation_edition.permanent_work_id]),
            work.pwids)
        edition, lp2 = self._edition(with_license_pool=True)
        work.license_pools.append(lp2)

        # Work.pwids finds both PWIDs.
        eq_(set([lp1.presentation_edition.permanent_work_id,
                 lp2.presentation_edition.permanent_work_id]),
            work.pwids)

    def test_open_access_for_permanent_work_id_no_licensepools(self):
        # There are no LicensePools, which short-circuilts
        # open_access_for_permanent_work_id.
        eq_(
            (None, False), Work.open_access_for_permanent_work_id(
                self._db, "No such permanent work ID", Edition.BOOK_MEDIUM,
                "eng"
            )
        )

        # Now it works.
        w = self._work(
            language="eng", with_license_pool=True,
            with_open_access_download=True
        )
        w.presentation_edition.permanent_work_id = "permid"
        eq_(
            (w, False), Work.open_access_for_permanent_work_id(
                self._db, "permid", Edition.BOOK_MEDIUM,
                "eng"
            )
        )

        # But the language, medium, and permanent ID must all match.
        eq_(
            (None, False), Work.open_access_for_permanent_work_id(
                self._db, "permid", Edition.BOOK_MEDIUM,
                "spa"
            )
        )

        eq_(
            (None, False), Work.open_access_for_permanent_work_id(
                self._db, "differentid", Edition.BOOK_MEDIUM,
                "eng"
            )
        )

        eq_(
            (None, False), Work.open_access_for_permanent_work_id(
                self._db, "differentid", Edition.AUDIO_MEDIUM,
                "eng"
            )
        )

    def test_open_access_for_permanent_work_id(self):
        # Two different works full of open-access license pools.
        w1 = self._work(with_license_pool=True, with_open_access_download=True)

        w2 = self._work(with_license_pool=True, with_open_access_download=True)

        [lp1] = w1.license_pools
        [lp2] = w2.license_pools

        # Work #2 has two different license pools grouped
        # together. Work #1 only has one.
        edition, lp3 = self._edition(
            with_license_pool=True, with_open_access_download=True
        )
        w2.license_pools.append(lp3)

        # Due to an error, it turns out both Works are providing the
        # exact same book.
        def mock_pwid(debug=False):
            return "abcd"
        for lp in [lp1, lp2, lp3]:
            lp.presentation_edition.permanent_work_id="abcd"
            lp.presentation_edition.calculate_permanent_work_id = mock_pwid

        # We've also got Work #3, which provides a commercial license
        # for that book.
        w3 = self._work(with_license_pool=True)
        w3_pool = w3.license_pools[0]
        w3_pool.presentation_edition.permanent_work_id="abcd"
        w3_pool.open_access = False

        # Work.open_access_for_permanent_work_id can resolve this problem.
        work, is_new = Work.open_access_for_permanent_work_id(
            self._db, "abcd", Edition.BOOK_MEDIUM, "eng"
        )

        # Work #3 still exists and its license pool was not affected.
        eq_([w3], self._db.query(Work).filter(Work.id==w3.id).all())
        eq_(w3, w3_pool.work)

        # But the other three license pools now have the same work.
        eq_(work, lp1.work)
        eq_(work, lp2.work)
        eq_(work, lp3.work)

        # Because work #2 had two license pools, and work #1 only had
        # one, work #1 was merged into work #2, rather than the other
        # way around.
        eq_(w2, work)
        eq_(False, is_new)

        # Work #1 no longer exists.
        eq_([], self._db.query(Work).filter(Work.id==w1.id).all())

        # Calling Work.open_access_for_permanent_work_id again returns the same
        # result.
        _db = self._db
        Work.open_access_for_permanent_work_id(_db, "abcd", Edition.BOOK_MEDIUM, "eng")
        eq_((w2, False), Work.open_access_for_permanent_work_id(
            self._db, "abcd", Edition.BOOK_MEDIUM, "eng"
        ))

    def test_open_access_for_permanent_work_id_can_create_work(self):

        # Here's a LicensePool with no corresponding Work.
        edition, lp = self._edition(with_license_pool=True)
        lp.open_access = True
        edition.permanent_work_id="abcd"

        # open_access_for_permanent_work_id creates the Work.
        work, is_new = Work.open_access_for_permanent_work_id(
            self._db, "abcd", Edition.BOOK_MEDIUM, edition.language
        )
        eq_([lp], work.license_pools)
        eq_(True, is_new)

    def test_potential_open_access_works_for_permanent_work_id(self):
        """Test of the _potential_open_access_works_for_permanent_work_id
        helper method.
        """

        # Here are two editions of the same book with the same PWID.
        title = 'Siddhartha'
        author = ['Herman Hesse']
        e1, lp1 = self._edition(
            data_source_name=DataSource.STANDARD_EBOOKS,
            title=title, authors=author, language='eng', with_license_pool=True,
        )
        e1.permanent_work_id = "pwid"

        e2, lp2 = self._edition(
            data_source_name=DataSource.GUTENBERG,
            title=title, authors=author, language='eng', with_license_pool=True,
        )
        e2.permanent_work_id = "pwid"

        w1 = Work()
        for lp in [lp1, lp2]:
            w1.license_pools.append(lp)
            lp.open_access = True

        def m():
            return Work._potential_open_access_works_for_permanent_work_id(
                self._db, "pwid", Edition.BOOK_MEDIUM, "eng"
            )
        pools, counts = m()

        # Both LicensePools show up in the list of LicensePools that
        # should be grouped together, and both LicensePools are
        # associated with the same Work.
        poolset = set([lp1, lp2])
        eq_(poolset, pools)
        eq_({w1 : 2}, counts)

        # Since the work was just created, it has no presentation
        # edition and thus no language. If the presentation edition
        # were set, the result would be the same.
        w1.presentation_edition = e1
        pools, counts = m()
        eq_(poolset, pools)
        eq_({w1 : 2}, counts)

        # If the Work's presentation edition has information that
        # _conflicts_ with the information passed in to
        # _potential_open_access_works_for_permanent_work_id, the Work
        # does not show up in `counts`, indicating that a new Work
        # should to be created to hold those books.
        bad_pe = self._edition()
        bad_pe.permanent_work_id='pwid'
        w1.presentation_edition = bad_pe

        bad_pe.language = 'fin'
        pools, counts = m()
        eq_(poolset, pools)
        eq_({}, counts)
        bad_pe.language = 'eng'

        bad_pe.medium = Edition.AUDIO_MEDIUM
        pools, counts = m()
        eq_(poolset, pools)
        eq_({}, counts)
        bad_pe.medium = Edition.BOOK_MEDIUM

        bad_pe.permanent_work_id = "Some other ID"
        pools, counts = m()
        eq_(poolset, pools)
        eq_({}, counts)
        bad_pe.permanent_work_id = "pwid"

        w1.presentation_edition = None

        # Now let's see what changes to a LicensePool will cause it
        # not to be eligible in the first place.
        def assert_lp1_missing():
            # A LicensePool that is not eligible will not show up in
            # the set and will not be counted towards the total of eligible
            # LicensePools for its Work.
            pools, counts = m()
            eq_(set([lp2]), pools)
            eq_({w1 : 1}, counts)

        # It has to be open-access.
        lp1.open_access = False
        assert_lp1_missing()
        lp1.open_access = True

        # The presentation edition's permanent work ID must match
        # what's passed into the helper method.
        e1.permanent_work_id = "another pwid"
        assert_lp1_missing()
        e1.permanent_work_id = "pwid"

        # The medium must also match.
        e1.medium = Edition.AUDIO_MEDIUM
        assert_lp1_missing()
        e1.medium = Edition.BOOK_MEDIUM

        # The language must also match.
        e1.language = "another language"
        assert_lp1_missing()
        e1.language = 'eng'

        # Finally, let's see what happens when there are two Works where
        # there should be one.
        w2 = Work()
        w2.license_pools.append(lp2)
        pools, counts = m()

        # This work is irrelevant and will not show up at all.
        w3 = Work()

        # Both Works have one associated LicensePool, so they have
        # equal claim to being 'the' Work for this work
        # ID/language/medium. The calling code will have to sort it
        # out.
        eq_(poolset, pools)
        eq_({w1: 1, w2: 1}, counts)

    def test_make_exclusive_open_access_for_permanent_work_id(self):
        # Here's a work containing an open-access LicensePool for
        # literary work "abcd".
        work1 = self._work(with_license_pool=True,
                          with_open_access_download=True)
        [abcd_oa] = work1.license_pools
        abcd_oa.presentation_edition.permanent_work_id="abcd"

        # Unfortunately, a commercial LicensePool for the literary
        # work "abcd" has gotten associated with the same work.
        edition, abcd_commercial = self._edition(
            with_license_pool=True, with_open_access_download=True
        )
        abcd_commercial.open_access = False
        abcd_commercial.presentation_edition.permanent_work_id="abcd"
        abcd_commercial.work = work1

        # Here's another Work containing an open-access LicensePool
        # for literary work "efgh".
        work2 = self._work(with_license_pool=True,
                          with_open_access_download=True)
        [efgh_1] = work2.license_pools
        efgh_1.presentation_edition.permanent_work_id="efgh"

        # Unfortunately, there's another open-access LicensePool for
        # "efgh", and it's incorrectly associated with the "abcd"
        # work.
        edition, efgh_2 = self._edition(
            with_license_pool=True, with_open_access_download=True
        )
        efgh_2.presentation_edition.permanent_work_id = "efgh"
        efgh_2.work = work1

        # Let's fix these problems.
        work1.make_exclusive_open_access_for_permanent_work_id(
            "abcd", Edition.BOOK_MEDIUM, "eng",
        )

        # The open-access "abcd" book is now the only LicensePool
        # associated with work1.
        eq_([abcd_oa], work1.license_pools)

        # Both open-access "efgh" books are now associated with work2.
        eq_(set([efgh_1, efgh_2]), set(work2.license_pools))

        # A third work has been created for the commercial edition of "abcd".
        assert abcd_commercial.work not in (work1, work2)

    def test_make_exclusive_open_access_for_null_permanent_work_id(self):
        # Here's a LicensePool that, due to a previous error, has
        # a null PWID in its presentation edition.
        work = self._work(with_license_pool=True,
                          with_open_access_download=True)
        [null1] = work.license_pools
        null1.presentation_edition.title = None
        null1.presentation_edition.sort_author = None
        null1.presentation_edition.permanent_work_id = None

        # Here's another LicensePool associated with the same work and
        # with the same problem.
        edition, null2 = self._edition(
            with_license_pool=True, with_open_access_download=True
        )
        work.license_pools.append(null2)

        for pool in work.license_pools:
            pool.presentation_edition.title = None
            pool.presentation_edition.sort_author = None
            pool.presentation_edition.permanent_work_id = None

        work.make_exclusive_open_access_for_permanent_work_id(
            None, Edition.BOOK_MEDIUM, edition.language
        )

        # Since a LicensePool with no PWID cannot have an associated Work,
        # this Work now have no LicensePools at all.
        eq_([], work.license_pools)

        eq_(None, null1.work)
        eq_(None, null2.work)

    def test_merge_into_success(self):
        # Here's a work with an open-access LicensePool.
        work1 = self._work(with_license_pool=True,
                           with_open_access_download=True)
        [lp1] = work1.license_pools
        lp1.presentation_edition.permanent_work_id="abcd"

        # Let's give it a WorkGenre and a WorkCoverageRecord.
        genre, ignore = Genre.lookup(self._db, "Fantasy")
        wg, wg_is_new = get_one_or_create(
            self._db, WorkGenre, work=work1, genre=genre
        )
        wcr, wcr_is_new = WorkCoverageRecord.add_for(work1, "test")

        # Here's another work with an open-access LicensePool for the
        # same book.
        work2 = self._work(with_license_pool=True,
                           with_open_access_download=True)
        [lp2] = work2.license_pools
        lp2.presentation_edition.permanent_work_id="abcd"

        # Let's merge the first work into the second.
        work1.merge_into(work2)

        # The first work has been deleted, as have its WorkGenre and
        # WorkCoverageRecord.
        eq_([], self._db.query(Work).filter(Work.id==work1.id).all())
        eq_([], self._db.query(WorkGenre).all())
        eq_([], self._db.query(WorkCoverageRecord).filter(
            WorkCoverageRecord.work_id==work1.id).all()
        )

    def test_open_access_for_permanent_work_id_fixes_mismatched_works_incidentally(self):

        # Here's a work with two open-access LicensePools for the book "abcd".
        work1 = self._work(with_license_pool=True,
                           with_open_access_download=True)
        [abcd_1] = work1.license_pools
        edition, abcd_2 = self._edition(
            with_license_pool=True, with_open_access_download=True
        )
        work1.license_pools.append(abcd_2)

        # Unfortunately, due to an earlier error, that work also
        # contains a _third_ open-access LicensePool, and this one
        # belongs to a totally separate book, "efgh".
        edition, efgh = self._edition(
            with_license_pool=True, with_open_access_download=True
        )
        work1.license_pools.append(efgh)

        # Here's another work with an open-access LicensePool for the
        # book "abcd".
        work2 = self._work(with_license_pool=True,
                           with_open_access_download=True)
        [abcd_3] = work2.license_pools

        # Unfortunately, this work also contains an open-access Licensepool
        # for the totally separate book, 'ijkl".
        edition, ijkl = self._edition(
            with_license_pool=True, with_open_access_download=True
        )
        work2.license_pools.append(ijkl)

        # Mock the permanent work IDs for all the presentation
        # editions in play.
        def mock_pwid_abcd(debug=False):
            return "abcd"

        def mock_pwid_efgh(debug=False):
            return "efgh"

        def mock_pwid_ijkl(debug=False):
            return "ijkl"

        for lp in abcd_1, abcd_2, abcd_3:
            lp.presentation_edition.calculate_permanent_work_id = mock_pwid_abcd
            lp.presentation_edition.permanent_work_id = 'abcd'

        efgh.presentation_edition.calculate_permanent_work_id = mock_pwid_efgh
        efgh.presentation_edition.permanent_work_id = 'efgh'

        ijkl.presentation_edition.calculate_permanent_work_id = mock_pwid_ijkl
        ijkl.presentation_edition.permanent_work_id = 'ijkl'

        # Calling Work.open_access_for_permanent_work_id()
        # automatically kicks the 'efgh' and 'ijkl' LicensePools into
        # their own works, and merges the second 'abcd' work with the
        # first one. (The first work is chosen because it represents
        # two LicensePools for 'abcd', not just one.)
        abcd_work, abcd_new = Work.open_access_for_permanent_work_id(
            self._db, "abcd", Edition.BOOK_MEDIUM, "eng"
        )
        efgh_work, efgh_new = Work.open_access_for_permanent_work_id(
            self._db, "efgh", Edition.BOOK_MEDIUM, "eng"
        )
        ijkl_work, ijkl_new = Work.open_access_for_permanent_work_id(
            self._db, "ijkl", Edition.BOOK_MEDIUM, "eng"
        )

        # We've got three different works here. The 'abcd' work is the
        # old 'abcd' work that had three LicensePools--the other work
        # was merged into it.
        eq_(abcd_1.work, abcd_work)
        assert efgh_work != abcd_work
        assert ijkl_work != abcd_work
        assert ijkl_work != efgh_work

        # The two 'new' works (for efgh and ijkl) are not counted as
        # new because they were created during the first call to
        # Work.open_access_for_permanent_work_id, when those
        # LicensePools were split out of Works where they didn't
        # belong.
        eq_(False, efgh_new)
        eq_(False, ijkl_new)

        eq_([ijkl], ijkl_work.license_pools)
        eq_([efgh], efgh_work.license_pools)
        eq_(3, len(abcd_work.license_pools))

    def test_open_access_for_permanent_work_untangles_tangled_works(self):

        # Here are three works for the books "abcd", "efgh", and "ijkl".
        abcd_work = self._work(with_license_pool=True,
                               with_open_access_download=True)
        [abcd_1] = abcd_work.license_pools

        efgh_work = self._work(with_license_pool=True,
                               with_open_access_download=True)
        [efgh_1] = efgh_work.license_pools

        # Unfortunately, due to an earlier error, the 'abcd' work
        # contains a LicensePool for 'efgh', and the 'efgh' work contains
        # a LicensePool for 'abcd'.
        #
        # (This is pretty much impossible, but bear with me...)

        abcd_edition, abcd_2 = self._edition(
            with_license_pool=True, with_open_access_download=True
        )
        efgh_work.license_pools.append(abcd_2)

        efgh_edition, efgh_2 = self._edition(
            with_license_pool=True, with_open_access_download=True
        )
        abcd_work.license_pools.append(efgh_2)

        # Both Works have a presentation edition that indicates the
        # permanent work ID is 'abcd'.
        abcd_work.presentation_edition = efgh_edition
        efgh_work.presentation_edition = efgh_edition

        def mock_pwid_abcd(debug=False):
            return "abcd"

        for lp in abcd_1, abcd_2:
            lp.presentation_edition.calculate_permanent_work_id = mock_pwid_abcd
            lp.presentation_edition.permanent_work_id = 'abcd'

        def mock_pwid_efgh(debug=False):
            return "efgh"

        for lp in efgh_1, efgh_2:
            lp.presentation_edition.calculate_permanent_work_id = mock_pwid_efgh
            lp.presentation_edition.permanent_work_id = 'efgh'

        # Calling Work.open_access_for_permanent_work_id() creates a
        # new work that contains both 'abcd' LicensePools.
        abcd_new, is_new = Work.open_access_for_permanent_work_id(
            self._db, "abcd", Edition.BOOK_MEDIUM, "eng"
        )
        eq_(True, is_new)
        eq_(set([abcd_1, abcd_2]), set(abcd_new.license_pools))

        # The old abcd_work now contains only the 'efgh' LicensePool
        # that didn't fit.
        eq_([efgh_2], abcd_work.license_pools)

        # We now have two works with 'efgh' LicensePools: abcd_work
        # and efgh_work. Calling
        # Work.open_access_for_permanent_work_id on 'efgh' will
        # consolidate the two LicensePools into one of the Works
        # (which one is nondeterministic).
        efgh_new, is_new = Work.open_access_for_permanent_work_id(
            self._db, "efgh", Edition.BOOK_MEDIUM, "eng"
        )
        eq_(False, is_new)
        eq_(set([efgh_1, efgh_2]), set(efgh_new.license_pools))
        assert efgh_new in (abcd_work, efgh_work)

        # The Work that was not chosen for consolidation now has no
        # LicensePools.
        if efgh_new is abcd_work:
            other = efgh_work
        else:
            other = abcd_work
        eq_([], other.license_pools)

    def test_merge_into_raises_exception_if_grouping_rules_violated(self):
        # Here's a work with an open-access LicensePool.
        work1 = self._work(with_license_pool=True,
                           with_open_access_download=True)
        [lp1] = work1.license_pools
        lp1.presentation_edition.permanent_work_id="abcd"

        # Here's another work with a commercial LicensePool for the
        # same book.
        work2 = self._work(with_license_pool=True,
                           with_open_access_download=True)
        [lp2] = work2.license_pools
        lp2.open_access = False
        lp2.presentation_edition.permanent_work_id="abcd"

        # The works cannot be merged.
        assert_raises_regexp(
            ValueError,
            "Refusing to merge .* into .* because it would put an open-access LicensePool into the same work as a non-open-access LicensePool.",
            work1.merge_into, work2,
        )

    def test_merge_into_raises_exception_if_pwids_differ(self):
        work1 = self._work(with_license_pool=True,
                           with_open_access_download=True)
        [abcd_oa] = work1.license_pools
        abcd_oa.presentation_edition.permanent_work_id="abcd"

        work2 = self._work(with_license_pool=True,
                           with_open_access_download=True)
        [efgh_oa] = work2.license_pools
        efgh_oa.presentation_edition.permanent_work_id="efgh"

        assert_raises_regexp(
            ValueError,
            "Refusing to merge .* into .* because permanent work IDs don't match: abcd vs. efgh",
            work1.merge_into,
            work2
        )

    def test_licensepool_without_identifier_gets_no_work(self):
        work = self._work(with_license_pool=True)
        [lp] = work.license_pools
        lp.identifier = None

        # Even if the LicensePool had a work before, it gets removed.
        eq_((None, False), lp.calculate_work())
        eq_(None, lp.work)

    def test_licensepool_without_presentation_edition_gets_no_work(self):
        work = self._work(with_license_pool=True)
        [lp] = work.license_pools

        # This LicensePool has no presentation edition and no way of
        # getting one.
        lp.presentation_edition = None
        lp.identifier.primarily_identifies = []

        # Even if the LicensePool had a work before, it gets removed.
        eq_((None, False), lp.calculate_work())
        eq_(None, lp.work)





class TestCoverResource(DatabaseTest):

    def test_set_cover(self):
        edition, pool = self._edition(with_license_pool=True)
        original = self._url
        mirror = self._url
        thumbnail_mirror = self._url
        sample_cover_path = self.sample_cover_path("test-book-cover.png")
        hyperlink, ignore = pool.add_link(
            Hyperlink.IMAGE, original, edition.data_source, "image/png",
            content=open(sample_cover_path).read())
        full_rep = hyperlink.resource.representation
        full_rep.set_as_mirrored(mirror)

        edition.set_cover(hyperlink.resource)
        eq_(mirror, edition.cover_full_url)
        eq_(None, edition.cover_thumbnail_url)

        # Now scale the cover.
        thumbnail, ignore = self._representation()
        thumbnail.thumbnail_of = full_rep
        thumbnail.set_as_mirrored(thumbnail_mirror)
        edition.set_cover(hyperlink.resource)
        eq_(mirror, edition.cover_full_url)
        eq_(thumbnail_mirror, edition.cover_thumbnail_url)

    def test_set_cover_for_very_small_image(self):
        edition, pool = self._edition(with_license_pool=True)
        original = self._url
        mirror = self._url
        sample_cover_path = self.sample_cover_path("tiny-image-cover.png")
        hyperlink, ignore = pool.add_link(
            Hyperlink.IMAGE, original, edition.data_source, "image/png",
            open(sample_cover_path).read())
        full_rep = hyperlink.resource.representation
        full_rep.set_as_mirrored(mirror)

        edition.set_cover(hyperlink.resource)
        eq_(mirror, edition.cover_full_url)
        eq_(mirror, edition.cover_thumbnail_url)

    def test_set_cover_for_smallish_image_uses_full_sized_image_as_thumbnail(self):
        edition, pool = self._edition(with_license_pool=True)
        original = self._url
        mirror = self._url
        sample_cover_path = self.sample_cover_path("tiny-image-cover.png")
        hyperlink, ignore = pool.add_link(
            Hyperlink.IMAGE, original, edition.data_source, "image/png",
            open(sample_cover_path).read())
        full_rep = hyperlink.resource.representation
        full_rep.set_as_mirrored(mirror)

        # For purposes of this test, pretend that the full-sized image is
        # larger than a thumbnail, but not terribly large.
        hyperlink.resource.representation.image_height = Edition.MAX_FALLBACK_THUMBNAIL_HEIGHT

        edition.set_cover(hyperlink.resource)
        eq_(mirror, edition.cover_full_url)
        eq_(mirror, edition.cover_thumbnail_url)

        # If the full-sized image had been slightly larger, we would have
        # decided not to use a thumbnail at all.
        hyperlink.resource.representation.image_height = Edition.MAX_FALLBACK_THUMBNAIL_HEIGHT + 1
        edition.cover_thumbnail_url = None
        edition.set_cover(hyperlink.resource)
        eq_(None, edition.cover_thumbnail_url)


    def test_attempt_to_scale_non_image_sets_scale_exception(self):
        rep, ignore = self._representation(media_type="text/plain", content="foo")
        scaled, ignore = rep.scale(300, 600, self._url, "image/png")
        expect = "ValueError: Cannot load non-image representation as image: type text/plain"
        assert scaled == rep
        assert expect in rep.scale_exception

    def test_cannot_scale_to_non_image(self):
        rep, ignore = self._representation(media_type="image/png", content="foo")
        assert_raises_regexp(
            ValueError,
            "Unsupported destination media type: text/plain",
            rep.scale, 300, 600, self._url, "text/plain")

    def test_success(self):
        cover = self.sample_cover_representation("test-book-cover.png")
        url = self._url
        thumbnail, is_new = cover.scale(300, 600, url, "image/png")
        eq_(True, is_new)
        eq_(url, thumbnail.url)
        eq_(None, thumbnail.mirror_url)
        eq_(None, thumbnail.mirrored_at)
        eq_(cover, thumbnail.thumbnail_of)
        eq_("image/png", thumbnail.media_type)
        eq_(300, thumbnail.image_height)
        eq_(200, thumbnail.image_width)

        # Try to scale the image to the same URL, and nothing will
        # happen, even though the proposed image size is
        # different.
        thumbnail2, is_new = cover.scale(400, 700, url, "image/png")
        eq_(thumbnail2, thumbnail)
        eq_(False, is_new)

        # Let's say the thumbnail has been mirrored.
        thumbnail.set_as_mirrored(self._url)

        old_content = thumbnail.content
        # With the force argument we can forcibly re-scale an image,
        # changing its size.
        eq_([thumbnail], cover.thumbnails)
        thumbnail2, is_new = cover.scale(
            400, 700, url, "image/png", force=True)
        eq_(True, is_new)
        eq_([thumbnail2], cover.thumbnails)
        eq_(cover, thumbnail2.thumbnail_of)

        # The same Representation, but now its data is different.
        eq_(thumbnail, thumbnail2)
        assert thumbnail2.content != old_content
        eq_(400, thumbnail.image_height)
        eq_(266, thumbnail.image_width)

        # The thumbnail has been regenerated, so it needs to be mirrored again.
        eq_(None, thumbnail.mirrored_at)

    def test_book_with_odd_aspect_ratio(self):
        # This book is 1200x600.
        cover = self.sample_cover_representation("childrens-book-cover.png")
        url = self._url
        thumbnail, is_new = cover.scale(300, 400, url, "image/png")
        eq_(True, is_new)
        eq_(url, thumbnail.url)
        eq_(cover, thumbnail.thumbnail_of)
        # The width was reduced to max_width, a reduction of a factor of three
        eq_(400, thumbnail.image_width)
        # The height was also reduced by a factory of three, even
        # though this takes it below max_height.
        eq_(200, thumbnail.image_height)

    def test_book_smaller_than_thumbnail_size(self):
        # This book is 200x200. No thumbnail will be created.
        cover = self.sample_cover_representation("tiny-image-cover.png")
        url = self._url
        thumbnail, is_new = cover.scale(300, 600, url, "image/png")
        eq_(False, is_new)
        eq_(thumbnail, cover)
        eq_([], cover.thumbnails)
        eq_(None, thumbnail.thumbnail_of)
        assert thumbnail.url != url

    def test_image_type_priority(self):
        """Test the image_type_priority method.

        All else being equal, we prefer some image types over
        others. Better image types get lower numbers.
        """
        m = Resource.image_type_priority
        eq_(None, m(None))
        eq_(None, m(Representation.EPUB_MEDIA_TYPE))

        png = m(Representation.PNG_MEDIA_TYPE)
        jpeg = m(Representation.JPEG_MEDIA_TYPE)
        gif = m(Representation.GIF_MEDIA_TYPE)
        svg = m(Representation.SVG_MEDIA_TYPE)

        assert png < jpeg
        assert jpeg < gif
        assert gif < svg

    def test_best_covers_among(self):
        # Here's a book with a thumbnail image.
        edition, pool = self._edition(with_license_pool=True)

        link1, ignore = pool.add_link(
            Hyperlink.THUMBNAIL_IMAGE, self._url, pool.data_source
        )
        resource_with_no_representation = link1.resource

        # A resource with no representation is not considered even if
        # it's the only option.
        eq_([], Resource.best_covers_among([resource_with_no_representation]))

        # Here's an abysmally bad cover.
        lousy_cover = self.sample_cover_representation("tiny-image-cover.png")
        lousy_cover.image_height=1
        lousy_cover.image_width=10000
        link2, ignore = pool.add_link(
            Hyperlink.THUMBNAIL_IMAGE, self._url, pool.data_source
        )
        resource_with_lousy_cover = link2.resource
        resource_with_lousy_cover.representation = lousy_cover

        # This cover is so bad that it's not even considered if it's
        # the only option.
        eq_([], Resource.best_covers_among([resource_with_lousy_cover]))

        # Here's a decent cover.
        decent_cover = self.sample_cover_representation("test-book-cover.png")
        link3, ignore = pool.add_link(
            Hyperlink.THUMBNAIL_IMAGE, self._url, pool.data_source
        )
        resource_with_decent_cover = link3.resource
        resource_with_decent_cover.representation = decent_cover

        # This cover is at least good enough to pass muster if there
        # is no other option.
        eq_(
            [resource_with_decent_cover],
            Resource.best_covers_among([resource_with_decent_cover])
        )

        # Let's create another cover image with identical
        # characteristics.
        link4, ignore = pool.add_link(
            Hyperlink.THUMBNAIL_IMAGE, self._url, pool.data_source
        )
        decent_cover_2 = self.sample_cover_representation("test-book-cover.png")
        resource_with_decent_cover_2 = link4.resource
        resource_with_decent_cover_2.representation = decent_cover_2

        l = [resource_with_decent_cover, resource_with_decent_cover_2]

        # best_covers_among() can't decide between the two -- they have
        # the same score.
        eq_(set(l), set(Resource.best_covers_among(l)))

        # All else being equal, if one cover is an PNG and the other
        # is a JPEG, we prefer the PNG.
        resource_with_decent_cover.representation.media_type = Representation.JPEG_MEDIA_TYPE
        eq_([resource_with_decent_cover_2], Resource.best_covers_among(l))

        # But if the metadata wrangler said to use the JPEG, we use the JPEG.
        metadata_wrangler = DataSource.lookup(
            self._db, DataSource.METADATA_WRANGLER
        )
        resource_with_decent_cover.data_source = metadata_wrangler

        # ...the decision becomes easy.
        eq_([resource_with_decent_cover], Resource.best_covers_among(l))

    def test_rejection_and_approval(self):
        # Create a Resource.
        edition, pool = self._edition(with_open_access_download=True)
        link = pool.add_link(Hyperlink.IMAGE, self._url, pool.data_source)[0]
        cover = link.resource

        # Give it all the right covers.
        cover_rep = self.sample_cover_representation("test-book-cover.png")
        thumbnail_rep = self.sample_cover_representation("test-book-cover.png")
        cover.representation = cover_rep
        cover_rep.thumbnails.append(thumbnail_rep)

        # Set its quality.
        cover.quality_as_thumbnail_image
        original_quality = cover.quality
        eq_(True, original_quality > 0)

        # Rejecting it sets the voted_quality and quality below zero.
        cover.reject()
        eq_(True, cover.voted_quality < 0)
        eq_(True, cover.quality < 0)

        # If the quality is already below zero, rejecting it doesn't
        # change the value.
        last_voted_quality = cover.voted_quality
        last_votes_for_quality = cover.votes_for_quality
        last_quality = cover.quality
        eq_(True, last_votes_for_quality > 0)
        cover.reject()
        eq_(last_voted_quality, cover.voted_quality)
        eq_(last_votes_for_quality, cover.votes_for_quality)
        eq_(last_quality, cover.quality)

        # If the quality is approved, the votes are updated as expected.
        cover.approve()
        eq_(0, cover.voted_quality)
        eq_(2, cover.votes_for_quality)
        # Because the number of human votes have gone up in contention,
        # the overall quality is lower than it was originally.
        eq_(True, cover.quality < original_quality)
        # But it's still above zero.
        eq_(True, cover.quality > 0)

        # Approving the cover again improves its quality further.
        last_quality = cover.quality
        cover.approve()
        eq_(True, cover.voted_quality > 0)
        eq_(3, cover.votes_for_quality)
        eq_(True, cover.quality > last_quality)

        # Rejecting the cover again will make the existing value negative.
        last_voted_quality = cover.voted_quality
        last_votes_for_quality = cover.votes_for_quality
        last_quality = cover.quality
        cover.reject()
        eq_(-last_voted_quality, cover.voted_quality)
        eq_(True, cover.quality < 0)

        eq_(last_votes_for_quality+1, cover.votes_for_quality)

    def test_quality_as_thumbnail_image(self):

        # Get some data sources ready, since a big part of image
        # quality comes from data source.
        gutenberg = DataSource.lookup(self._db, DataSource.GUTENBERG)
        gutenberg_cover_generator = DataSource.lookup(
            self._db, DataSource.GUTENBERG_COVER_GENERATOR
        )
        overdrive = DataSource.lookup(self._db, DataSource.OVERDRIVE)
        metadata_wrangler = DataSource.lookup(
            self._db, DataSource.METADATA_WRANGLER
        )

        # Here's a book with a thumbnail image.
        edition, pool = self._edition(with_license_pool=True)
        hyperlink, ignore = pool.add_link(
            Hyperlink.THUMBNAIL_IMAGE, self._url, overdrive
        )
        resource = hyperlink.resource

        # Without a representation, the thumbnail image is useless.
        eq_(0, resource.quality_as_thumbnail_image)

        ideal_height = Identifier.IDEAL_IMAGE_HEIGHT
        ideal_width = Identifier.IDEAL_IMAGE_WIDTH

        cover = self.sample_cover_representation("tiny-image-cover.png")
        resource.representation = cover
        eq_(1.0, resource.quality_as_thumbnail_image)

        # Changing the image aspect ratio affects the quality as per
        # thumbnail_size_quality_penalty.
        cover.image_height = ideal_height * 2
        cover.image_width = ideal_width
        eq_(0.5, resource.quality_as_thumbnail_image)

        # Changing the data source also affects the quality. Gutenberg
        # covers are penalized heavily...
        cover.image_height = ideal_height
        cover.image_width = ideal_width
        resource.data_source = gutenberg
        eq_(0.5, resource.quality_as_thumbnail_image)

        # The Gutenberg cover generator is penalized less heavily.
        resource.data_source = gutenberg_cover_generator
        eq_(0.6, resource.quality_as_thumbnail_image)

        # The metadata wrangler actually gets a _bonus_, to encourage the
        # use of its covers over those provided by license sources.
        resource.data_source = metadata_wrangler
        eq_(2, resource.quality_as_thumbnail_image)

    def test_thumbnail_size_quality_penalty(self):
        """Verify that Representation._cover_size_quality_penalty penalizes
        images that are the wrong aspect ratio, or too small.
        """

        ideal_ratio = Identifier.IDEAL_COVER_ASPECT_RATIO
        ideal_height = Identifier.IDEAL_IMAGE_HEIGHT
        ideal_width = Identifier.IDEAL_IMAGE_WIDTH

        def f(width, height):
            return Representation._thumbnail_size_quality_penalty(width, height)

        # In the absence of any size information we assume
        # everything's fine.
        eq_(1, f(None, None))

        # The perfect image has no penalty.
        eq_(1, f(ideal_width, ideal_height))

        # An image that is the perfect aspect ratio, but too large,
        # has no penalty.
        eq_(1, f(ideal_width*2, ideal_height*2))

        # An image that is the perfect aspect ratio, but is too small,
        # is penalised.
        eq_(1/4.0, f(ideal_width*0.5, ideal_height*0.5))
        eq_(1/16.0, f(ideal_width*0.25, ideal_height*0.25))

        # An image that deviates from the perfect aspect ratio is
        # penalized in proportion.
        eq_(1/2.0, f(ideal_width*2, ideal_height))
        eq_(1/2.0, f(ideal_width, ideal_height*2))
        eq_(1/4.0, f(ideal_width*4, ideal_height))
        eq_(1/4.0, f(ideal_width, ideal_height*4))














class TestSiteConfigurationHasChanged(DatabaseTest):

    class MockSiteConfigurationHasChanged(object):
        """Keep track of whether site_configuration_has_changed was
        ever called.
        """
        def __init__(self):
            self.was_called = False

        def run(self, _db):
            self.was_called = True
            site_configuration_has_changed(_db)

        def assert_was_called(self):
            "Assert that `was_called` is True, then reset it for the next assertion."
            assert self.was_called
            self.was_called = False

        def assert_was_not_called(self):
            assert not self.was_called

    def setup(self):
        super(TestSiteConfigurationHasChanged, self).setup()

        # Mock model.site_configuration_has_changed
        self.old_site_configuration_has_changed = model.site_configuration_has_changed
        self.mock = self.MockSiteConfigurationHasChanged()
        for module in model, lane:
            module.site_configuration_has_changed = self.mock.run

    def teardown(self):
        super(TestSiteConfigurationHasChanged, self).teardown()
        for module in model, lane:
            module.site_configuration_has_changed = self.old_site_configuration_has_changed

    def test_site_configuration_has_changed(self):
        """Test the site_configuration_has_changed() function and its
        effects on the Configuration object.
        """
        # The database configuration timestamp is initialized as part
        # of the default data. In that case, it happened during the
        # package_setup() for this test run.
        last_update = Configuration.site_configuration_last_update(self._db)

        timestamp_value = Timestamp.value(
            self._db, Configuration.SITE_CONFIGURATION_CHANGED, None
        )
        eq_(timestamp_value, last_update)

        # Now let's call site_configuration_has_changed().
        time_of_update = datetime.datetime.utcnow()
        site_configuration_has_changed(self._db, timeout=0)

        # The Timestamp has changed in the database.
        new_timestamp_value = Timestamp.value(
            self._db, Configuration.SITE_CONFIGURATION_CHANGED, None
        )
        assert new_timestamp_value > timestamp_value

        # The locally-stored last update value has been updated.
        new_last_update_time = Configuration.site_configuration_last_update(
            self._db, timeout=0
        )
        assert new_last_update_time > last_update
        assert (new_last_update_time - time_of_update).total_seconds() < 1

        # Let's be sneaky and update the timestamp directly,
        # without calling site_configuration_has_changed(). This
        # simulates another process on a different machine calling
        # site_configuration_has_changed() -- they will know about the
        # change but we won't be informed.
        timestamp = Timestamp.stamp(
            self._db, Configuration.SITE_CONFIGURATION_CHANGED, None
        )

        # Calling Configuration.check_for_site_configuration_update
        # doesn't detect the change because by default we only go to
        # the database every ten minutes.
        eq_(new_last_update_time,
            Configuration.site_configuration_last_update(self._db))

        # Passing in a different timeout value forces the method to go
        # to the database and find the correct answer.
        newer_update = Configuration.site_configuration_last_update(
            self._db, timeout=0
        )
        assert newer_update > last_update

        # It's also possible to change the timeout value through a
        # site-wide ConfigurationSetting
        ConfigurationSetting.sitewide(
            self._db, Configuration.SITE_CONFIGURATION_TIMEOUT
        ).value = 0
        timestamp = Timestamp.stamp(
            self._db, Configuration.SITE_CONFIGURATION_CHANGED, None
        )
        even_newer_update = Configuration.site_configuration_last_update(
            self._db, timeout=0
        )
        assert even_newer_update > newer_update


        # If ConfigurationSettings are updated twice within the
        # timeout period (default 1 second), the last update time is
        # only set once, to avoid spamming the Timestamp with updates.

        # The high site-wide value for 'timeout' saves this code. If we decided
        # that the timeout had expired and tried to check the
        # Timestamp, the code would crash because we're not passing
        # a database connection in.
        site_configuration_has_changed(None, timeout=100)

        # Nothing has changed -- how could it, with no database connection
        # to modify anything?
        eq_(even_newer_update,
            Configuration.site_configuration_last_update(self._db))

    # We don't test every event listener, but we do test one of each type.
    def test_configuration_relevant_lifecycle_event_updates_configuration(self):
        """When you create or modify a relevant item such as a
        ConfigurationSetting, site_configuration_has_changed is called.
        """
        ConfigurationSetting.sitewide(self._db, "setting").value = "value"
        self.mock.assert_was_called()

        ConfigurationSetting.sitewide(self._db, "setting").value = "value2"
        self.mock.assert_was_called()

    def test_lane_change_updates_configuration(self):
        """Verify that configuration-relevant changes work the same way
        in the lane module as they do in the model module.
        """
        lane = self._lane()
        self.mock.assert_was_called()

        lane.add_genre("Science Fiction")
        self.mock.assert_was_called()

    def test_configuration_relevant_collection_change_updates_configuration(self):
        """When you add a relevant item to a SQLAlchemy collection, such as
        adding a Collection to library.collections,
        site_configuration_has_changed is called.
        """

        # Creating a collection calls the method via an 'after_insert'
        # event on Collection.
        library = self._default_library
        collection = self._collection()
        self._db.commit()
        self.mock.assert_was_called()

        # Adding the collection to the library calls the method via
        # an 'append' event on Collection.libraries.
        library.collections.append(collection)
        self._db.commit()
        self.mock.assert_was_called()

        # Associating a CachedFeed with the library does _not_ call
        # the method, because nothing changed on the Library object and
        # we don't listen for 'append' events on Library.cachedfeeds.
        create(self._db, CachedFeed, type='page', pagination='',
               facets='', library=library)
        self._db.commit()
        self.mock.assert_was_not_called()


class TestCollectionForMetadataWrangler(DatabaseTest):

    """Tests that requirements to the metadata wrangler's use of Collection
    are being met by continued development on the Collection class.

    If any of these tests are failing, development will be required on the
    metadata wrangler to meet the needs of the new Collection class.
    """

    def test_only_name_is_required(self):
        """Test that only name is a required field on
        the Collection class.
        """
        collection = create(
            self._db, Collection, name='banana'
        )[0]
        eq_(True, isinstance(collection, Collection))




class TestMaterializedViews(DatabaseTest):

    def test_license_pool_is_works_preferred_license_pool(self):
        """Verify that the license_pool_id stored in the materialized views
        identifies the LicensePool associated with the Work's
        presentation edition, not some other LicensePool.
        """
        # Create a Work with two LicensePools
        work = self._work(with_license_pool=True)
        [pool1] = work.license_pools
        edition2, pool2 = self._edition(with_license_pool=True)
        work.license_pools.append(pool1)
        eq_([pool1], work.presentation_edition.license_pools)
        work.presentation_ready = True
        work.simple_opds_entry = '<entry>'
        work.assign_genres_from_weights({classifier.Fantasy : 1})

        # Make sure the Work shows up in the materialized view.
        SessionManager.refresh_materialized_views(self._db)

        from model import MaterializedWorkWithGenre as mwgc
        [mwg] = self._db.query(mwgc).all()

        eq_(pool1.id, mwg.license_pool_id)

        # If we change the Work's preferred edition, we change the
        # license_pool_id that gets stored in the materialized views.
        work.set_presentation_edition(edition2)
        SessionManager.refresh_materialized_views(self._db)
        [mwg] = self._db.query(mwgc).all()

        eq_(pool2.id, mwg.license_pool_id)

    def test_license_data_source_is_stored_in_views(self):
        """Verify that the data_source_name stored in the materialized view
        is the DataSource associated with the LicensePool, not the
        DataSource associated with the presentation Edition.
        """

        # Create a Work whose LicensePool has three Editions: one from
        # Gutenberg (created by default), one from the admin interface
        # (created manually), and one generated by the presentation
        # edition generator, which synthesizes the other two.
        work = self._work(with_license_pool=True)

        [pool] = work.license_pools
        gutenberg_edition = pool.presentation_edition

        identifier = pool.identifier
        staff_edition = self._edition(
            data_source_name=DataSource.LIBRARY_STAFF,
            identifier_type=identifier.type,
            identifier_id=identifier.identifier
        )
        staff_edition.title = u"staff chose this title"
        staff_edition.sort_title = u"staff chose this title"
        pool.set_presentation_edition()
        work.set_presentation_edition(pool.presentation_edition)

        # The presentation edition has the title taken from the admin
        # interface, but it was created by the presentation edition
        # generator.
        presentation_edition = pool.presentation_edition
        eq_("staff chose this title", presentation_edition.title)
        eq_(DataSource.PRESENTATION_EDITION,
            presentation_edition.data_source.name
        )

        # Make sure the Work will show up in the materialized view.
        work.presentation_ready = True
        work.simple_opds_entry = '<entry>'
        work.assign_genres_from_weights({classifier.Fantasy : 1})

        SessionManager.refresh_materialized_views(self._db)

        from model import MaterializedWorkWithGenre as mwgc
        [mwg] = self._db.query(mwgc).all()

        # We would expect the data source to be Gutenberg, since
        # that's the edition associated with the LicensePool, and not
        # the data source of the Work's presentation edition.
        eq_(pool.data_source.name, mwg.name)

        # However, we would expect the title of the work to come from
        # the presentation edition.
        eq_("staff chose this title", mwg.sort_title)

        # And since the data_source_id is the ID of the data source
        # associated with the license pool, we would expect it to be
        # the data source ID of the license pool.
        eq_(pool.data_source.id, mwg.data_source_id)

    def test_work_on_same_list_twice(self):
        # Here's the NYT best-seller list.
        cl, ignore = self._customlist(num_entries=0)

        # Here are two Editions containing data from the NYT
        # best-seller list.
        now = datetime.datetime.utcnow()
        earlier = now - datetime.timedelta(seconds=3600)
        edition1 = self._edition()
        entry1, ignore = cl.add_entry(edition1, first_appearance=earlier)

        edition2 = self._edition()
        entry2, ignore = cl.add_entry(edition2, first_appearance=now)

        # In a shocking turn of events, we've determined that the two
        # editions are slight title variants of the same work.
        romance, ignore = Genre.lookup(self._db, "Romance")
        work = self._work(with_license_pool=True, genre=romance)
        entry1.work = work
        entry2.work = work
        self._db.commit()

        # The materialized view can handle this revelation
        # and stores the two list entries in different rows.
        SessionManager.refresh_materialized_views(self._db)
        from model import MaterializedWorkWithGenre as mw
        [o1, o2] = self._db.query(mw).order_by(mw.list_edition_id)

        # Both MaterializedWorkWithGenre objects are on the same
        # list, associated with the same work, the same genre,
        # and the same presentation edition.
        for o in (o1, o2):
            eq_(cl.id, o.list_id)
            eq_(work.id, o.works_id)
            eq_(romance.id, o.genre_id)
            eq_(work.presentation_edition.id, o.editions_id)

        # But they are associated with different list editions.
        eq_(edition1.id, o1.list_edition_id)
        eq_(edition2.id, o2.list_edition_id)



class TestTupleToNumericrange(object):
    """Test the tuple_to_numericrange helper function."""

    def test_tuple_to_numericrange(self):
        f = tuple_to_numericrange
        eq_(None, f(None))

        one_to_ten = f((1,10))
        assert isinstance(one_to_ten, NumericRange)
        eq_(1, one_to_ten.lower)
        eq_(10, one_to_ten.upper)
        eq_(True, one_to_ten.upper_inc)

        up_to_ten = f((None, 10))
        assert isinstance(up_to_ten, NumericRange)
        eq_(None, up_to_ten.lower)
        eq_(10, up_to_ten.upper)
        eq_(True, up_to_ten.upper_inc)

        ten_and_up = f((10,None))
        assert isinstance(ten_and_up, NumericRange)
        eq_(10, ten_and_up.lower)
        eq_(None, ten_and_up.upper)
        eq_(False, ten_and_up.upper_inc)



class MockHasTableCache(HasFullTableCache):

    """A simple HasFullTableCache that returns the same cache key
    for every object.
    """

    _cache = HasFullTableCache.RESET
    _id_cache = HasFullTableCache.RESET

    ID = "the only ID"
    KEY = "the only cache key"

    @property
    def id(self):
        return self.ID

    def cache_key(self):
        return self.KEY


class TestHasFullTableCache(DatabaseTest):

    def setup(self):
        super(TestHasFullTableCache, self).setup()
        self.mock_class = MockHasTableCache
        self.mock = MockHasTableCache()
        self.mock._cache = HasFullTableCache.RESET

    def test_reset_cache(self):
        self.mock_class._cache = object()
        self.mock_class._id_cache = object()
        self.mock_class.reset_cache()
        eq_(HasFullTableCache.RESET, self.mock_class._cache)
        eq_(HasFullTableCache.RESET, self.mock_class._id_cache)

    def test_cache_insert(self):
        temp_cache = {}
        temp_id_cache = {}
        self.mock_class._cache_insert(self.mock, temp_cache, temp_id_cache)
        eq_({MockHasTableCache.KEY: self.mock}, temp_cache)
        eq_({MockHasTableCache.ID: self.mock}, temp_id_cache)

    # populate_cache(), by_cache_key(), and by_id() are tested in
    # TestGenre since those methods must be backed by a real database
    # table.
