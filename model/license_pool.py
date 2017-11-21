# encoding: utf-8
from . import Base
from sqlalchemy import (
    Boolean,
    Column,
    DateTime,
    ForeignKey,
    Index,
    Integer,
    Unicode,
    UniqueConstraint,
)

from sqlalchemy.orm import (
    relationship
)


class LicensePool(Base):
    """A pool of undifferentiated licenses for a work from a given source.
    """

    __tablename__ = 'licensepools'
    id = Column(Integer, primary_key=True)

    # A LicensePool may be associated with a Work. (If it's not, no one
    # can check it out.)
    work_id = Column(Integer, ForeignKey('works.id'), index=True)

    # Each LicensePool is associated with one DataSource and one
    # Identifier.
    data_source_id = Column(Integer, ForeignKey('datasources.id'), index=True)
    identifier_id = Column(Integer, ForeignKey('identifiers.id'), index=True)

    # Each LicensePool belongs to one Collection.
    collection_id = Column(Integer, ForeignKey('collections.id'),
                           index=True, nullable=False)
    
    # Each LicensePool has an Edition which contains the metadata used
    # to describe this book.
    presentation_edition_id = Column(Integer, ForeignKey('editions.id'), index=True)

    # One LicensePool can have many Loans.
    loans = relationship('Loan', backref='license_pool')

    # One LicensePool can have many Holds.
    holds = relationship('Hold', backref='license_pool')

    # One LicensePool can have many CirculationEvents
    circulation_events = relationship(
        "CirculationEvent", backref="license_pool")

    # One LicensePool can be associated with many Complaints.
    complaints = relationship('Complaint', backref='license_pool')

    # The date this LicensePool was first created in our db
    # (the date we first discovered that ​we had that book in ​our collection).
    availability_time = Column(DateTime, index=True)

    # A LicensePool may be superceded by some other LicensePool
    # associated with the same Work. This may happen if it's an
    # open-access LicensePool and a better-quality version of the same
    # book is available from another Open-Access source.
    superceded = Column(Boolean, default=False)

    # A LicensePool that seemingly looks fine may be manually suppressed
    # to be temporarily or permanently removed from the collection.
    suppressed = Column(Boolean, default=False, index=True)

    # A textual description of a problem with this license pool
    # that caused us to suppress it.
    license_exception = Column(Unicode, index=True)

    open_access = Column(Boolean, index=True)
    last_checked = Column(DateTime, index=True)
    licenses_owned = Column(Integer,default=0)
    licenses_available = Column(Integer,default=0, index=True)
    licenses_reserved = Column(Integer,default=0)
    patrons_in_hold_queue = Column(Integer,default=0)

    # This lets us cache the work of figuring out the best open access
    # link for this LicensePool.
    _open_access_download_url = Column(Unicode, name="open_access_download_url")
    
    # A Collection can not have more than one LicensePool for a given
    # Identifier from a given DataSource.
    __table_args__ = (
        UniqueConstraint('identifier_id', 'data_source_id', 'collection_id'),
    )

    @property
    def delivery_mechanisms(self):
        """Find all LicensePoolDeliveryMechanisms for this LicensePool.        
        """
        _db = Session.object_session(self)
        LPDM = LicensePoolDeliveryMechanism
        return _db.query(LPDM).filter(
            LPDM.data_source==self.data_source).filter(
                LPDM.identifier==self.identifier)
    
    def __repr__(self):
        if self.identifier:
            identifier = "%s/%s" % (self.identifier.type, 
                                    self.identifier.identifier)
        else:
            identifier = "unknown identifier"
        return "<LicensePool #%s for %s: owned=%d available=%d reserved=%d holds=%d>" % (
            self.id, identifier, self.licenses_owned, self.licenses_available, 
            self.licenses_reserved, self.patrons_in_hold_queue
        )

    @classmethod
    def for_foreign_id(self, _db, data_source, foreign_id_type, foreign_id,
                       rights_status=None, collection=None, autocreate=True):
        """Find or create a LicensePool for the given foreign ID."""

        if not collection:
            raise CollectionMissing()
        
        # Get the DataSource.
        if isinstance(data_source, basestring):
            data_source = DataSource.lookup(_db, data_source)

        # The type of the foreign ID must be the primary identifier
        # type for the data source.
        if (data_source.primary_identifier_type and 
            foreign_id_type != data_source.primary_identifier_type
            and foreign_id_type != Identifier.DEPRECATED_NAMES.get(data_source.primary_identifier_type)
        ):
            raise ValueError(
                "License pools for data source '%s' are keyed to "
                "identifier type '%s' (not '%s', which was provided)" % (
                    data_source.name, data_source.primary_identifier_type,
                    foreign_id_type
                )
            )
 
        # Get the Identifier.
        identifier, ignore = Identifier.for_foreign_id(
            _db, foreign_id_type, foreign_id
            )

        kw = dict(data_source=data_source, identifier=identifier,
                  collection=collection)
        if rights_status:
            kw['rights_status'] = rights_status

        # Get the LicensePool that corresponds to the
        # DataSource/Identifier/Collection.
        if autocreate:
            license_pool, was_new = get_one_or_create(_db, LicensePool, **kw)
        else:
            license_pool = get_one(_db, LicensePool, **kw)
            was_new = False
            
        if was_new and not license_pool.availability_time:
            now = datetime.datetime.utcnow()
            license_pool.availability_time = now

        if was_new:
            # Set the LicensePool's initial values to indicate
            # that we don't actually know how many copies we own.
            license_pool.licenses_owned = 0
            license_pool.licenses_available = 0
            license_pool.licenses_reserved = 0
            license_pool.patrons_in_hold_queue = 0

        return license_pool, was_new

    @classmethod
    def with_no_work(cls, _db):
        """Find LicensePools that have no corresponding Work."""
        return _db.query(LicensePool).outerjoin(Work).filter(
            Work.id==None).all()

    @property
    def deliverable(self):
        """This LicensePool can actually be delivered to patrons.
        """
        return (
            (self.open_access or self.licenses_owned > 0)
            and any(
                [dm.delivery_mechanism.default_client_can_fulfill
                for dm in self.delivery_mechanisms]
            )
        )

    @classmethod
    def with_complaint(cls, library, resolved=False):
        """Return query for LicensePools that have at least one Complaint."""
        _db = Session.object_session(library)
        subquery = _db.query(
                LicensePool.id,
                func.count(LicensePool.id).label("complaint_count")
            ).select_from(LicensePool).join(
                LicensePool.collection).join(
                    Collection.libraries).filter(
                        Library.id==library.id
                    ).join(
                        LicensePool.complaints
                    ).group_by(
                        LicensePool.id
                    )

        if resolved == False:
            subquery = subquery.filter(Complaint.resolved == None)
        elif resolved == True:
            subquery = subquery.filter(Complaint.resolved != None)

        subquery = subquery.subquery()

        return _db.query(LicensePool).\
            join(subquery, LicensePool.id == subquery.c.id).\
            order_by(subquery.c.complaint_count.desc()).\
            add_columns(subquery.c.complaint_count)

    @property
    def open_access_source_priority(self):
        """What priority does this LicensePool's DataSource have in
        our list of open-access content sources?
        
        e.g. GITenberg books are prefered over Gutenberg books,
        because there's a defined process for fixing errors and they
        are more likely to have good cover art.
        """
        try:
            priority = DataSource.OPEN_ACCESS_SOURCE_PRIORITY.index(
                self.data_source.name
            )
        except ValueError, e:
            # The source of this download is not mentioned in our
            # priority list. Treat it as the lowest priority.
            priority = -1
        return priority

    def better_open_access_pool_than(self, champion):
        """ Is this open-access pool generally known for better-quality
        download files than the passed-in pool?
        """
        # A license pool with no identifier shouldn't happen, but it
        # definitely shouldn't be considered.
        if not self.identifier:
            return False

        # A suppressed license pool should never be used, even if there is
        # no alternative.
        if self.suppressed:
            return False

        # A non-open-access license pool is not eligible for consideration.
        if not self.open_access:
            return False

        # At this point we have a LicensePool that is at least
        # better than nothing.
        if not champion:
            return True

        challenger_resource = self.best_open_access_link
        if not challenger_resource:
            # This LicensePool is supposedly open-access but we don't
            # actually know where the book is. It will be chosen only
            # if there is no alternative.
            return False

        champion_priority = champion.open_access_source_priority
        challenger_priority = self.open_access_source_priority

        if challenger_priority > champion_priority:
            return True

        if challenger_priority < champion_priority:
            return False

        if (self.data_source.name == DataSource.GUTENBERG
            and champion.data_source == self.data_source):
            # These two LicensePools are both from Gutenberg, and
            # normally this wouldn't matter, but higher Gutenberg
            # numbers beat lower Gutenberg numbers.
            champion_id = int(champion.identifier.identifier)
            challenger_id = int(self.identifier.identifier)

            if challenger_id > champion_id:
                logging.info(
                    "Gutenberg %d beats Gutenberg %d",
                    challenger_id, champion_id
                )
                return True
        return False

    def set_open_access_status(self):
        """Set .open_access based on whether there is currently
        an open-access LicensePoolDeliveryMechanism for this LicensePool.
        """
        for dm in self.delivery_mechanisms:
            if dm.is_open_access:
                self.open_access = True
                break
        else:
            self.open_access = False

    def set_presentation_edition(self, equivalent_editions=None):
        """Create or update the presentation Edition for this LicensePool.

        The presentation Edition is made of metadata from all Editions
        associated with the LicensePool's identifier.

        :param equivalent_editions: An optional list of Edition objects
        that don't share this LicensePool's identifier but are associated
        with its equivalent identifiers in some way. This option is used
        to create Works on the Metadata Wrangler.

        :return: A boolean explaining whether any of the presentation
        information associated with this LicensePool actually changed.
        """
        _db = Session.object_session(self)
        old_presentation_edition = self.presentation_edition
        changed = False

        editions = equivalent_editions
        if not editions:
            editions = self.identifier.primarily_identifies
        all_editions = list(Edition.sort_by_priority(editions))

        # Note: We can do a cleaner solution, if we refactor to not use metadata's 
        # methods to update editions.  For now, we're choosing to go with the below approach.
        from metadata_layer import (
            Metadata, 
            IdentifierData, 
            ReplacementPolicy,
        )

        if len(all_editions) == 1:
            # There's only one edition associated with this
            # LicensePool. Use it as the presentation edition rather
            # than creating an identical composite.
            self.presentation_edition = all_editions[0]
        else:
            edition_identifier = IdentifierData(self.identifier.type, self.identifier.identifier)
            metadata = Metadata(data_source=DataSource.PRESENTATION_EDITION, primary_identifier=edition_identifier)

            for edition in all_editions:
                if (edition.data_source.name != DataSource.PRESENTATION_EDITION):
                    metadata.update(Metadata.from_edition(edition))

            # Note: Since this is a presentation edition it does not have a
            # license data source, even if one of the editions it was
            # created from does have a license data source.
            metadata._license_data_source = None
            metadata.license_data_source_obj = None
            edition, is_new = metadata.edition(_db)

            policy = ReplacementPolicy.from_metadata_source()
            self.presentation_edition, edition_core_changed = metadata.apply(
                edition, collection=self.collection, replace=policy
            )
            changed = changed or edition_core_changed

        presentation_changed = self.presentation_edition.calculate_presentation()
        changed = changed or presentation_changed

        # if the license pool is associated with a work, and the work currently has no presentation edition, 
        # then do a courtesy call to the work, and tell it about the presentation edition.
        if self.work and not self.work.presentation_edition:
            self.work.set_presentation_edition(self.presentation_edition)

        return (
            self.presentation_edition != old_presentation_edition 
            or changed
        )

    def add_link(self, rel, href, data_source, media_type=None,
                 content=None, content_path=None):
        """Add a link between this LicensePool and a Resource.

        :param rel: The relationship between this LicensePool and the resource
               on the other end of the link.
        :param href: The URI of the resource on the other end of the link.
        :param media_type: Media type of the representation associated
               with the resource.
        :param content: Content of the representation associated with the
               resource.
        :param content_path: Path (relative to DATA_DIRECTORY) of the
               representation associated with the resource.
        """
        return self.identifier.add_link(
            rel, href, data_source, media_type, content, content_path)

    def needs_update(self):
        """Is it time to update the circulation info for this license pool?"""
        now = datetime.datetime.utcnow()
        if not self.last_checked:
            # This pool has never had its circulation info checked.
            return True
        maximum_stale_time = self.data_source.extra.get(
            'circulation_refresh_rate_seconds')
        if maximum_stale_time is None:
            # This pool never needs to have its circulation info checked.
            return False
        age = now - self.last_checked
        return age > maximum_stale_time

    def update_availability(
            self, new_licenses_owned, new_licenses_available, 
            new_licenses_reserved, new_patrons_in_hold_queue,
            analytics=None, as_of=None):
        """Update the LicensePool with new availability information.
        Log the implied changes with the analytics provider.
        """
        changes_made = False
        _db = Session.object_session(self)
        if not as_of:
            as_of = datetime.datetime.utcnow()
        elif as_of == CirculationEvent.NO_DATE:
            # The caller explicitly does not want
            # LicensePool.last_checked to be updated.
            as_of = None

        old_licenses_owned = self.licenses_owned
        old_licenses_available = self.licenses_available
        old_licenses_reserved = self.licenses_reserved
        old_patrons_in_hold_queue = self.patrons_in_hold_queue

        for old_value, new_value, more_event, fewer_event in (
                [self.patrons_in_hold_queue,  new_patrons_in_hold_queue,
                 CirculationEvent.DISTRIBUTOR_HOLD_PLACE, CirculationEvent.DISTRIBUTOR_HOLD_RELEASE],
                [self.licenses_available, new_licenses_available,
                 CirculationEvent.DISTRIBUTOR_CHECKIN, CirculationEvent.DISTRIBUTOR_CHECKOUT],
                [self.licenses_reserved, new_licenses_reserved,
                 CirculationEvent.DISTRIBUTOR_AVAILABILITY_NOTIFY, None],
                [self.licenses_owned, new_licenses_owned,
                 CirculationEvent.DISTRIBUTOR_LICENSE_ADD,
                 CirculationEvent.DISTRIBUTOR_LICENSE_REMOVE]):
            if new_value is None:
                continue
            if old_value == new_value:
                continue
            changes_made = True

            if old_value < new_value:
                event_name = more_event
            else:
                event_name = fewer_event

            if not event_name:
                continue

            self.collect_analytics_event(
                analytics, event_name, as_of, old_value, new_value
            )

        # Update the license pool with the latest information.
        any_data = False
        if new_licenses_owned is not None:
            self.licenses_owned = new_licenses_owned
            any_data = True
        if new_licenses_available is not None:
            self.licenses_available = new_licenses_available
            any_data = True
        if new_licenses_reserved is not None:
            self.licenses_reserved = new_licenses_reserved
            any_data = True
        if new_patrons_in_hold_queue is not None:
            self.patrons_in_hold_queue = new_patrons_in_hold_queue
            any_data = True

        if as_of and (any_data or changes_made):
            # Sometimes update_availability is called with no actual
            # numbers, but that's not the case this time. We got
            # numbers and they may have even changed our view of the
            # LicensePool.
            self.last_checked = as_of
            if self.work:
                self.work.last_update_time = as_of

        if changes_made:
            message, args = self.circulation_changelog(
                old_licenses_owned, old_licenses_available,
                old_licenses_reserved, old_patrons_in_hold_queue
            )
            logging.info(message, *args)

        return changes_made

    def collect_analytics_event(self, analytics, event_name, as_of,
                                old_value, new_value):
        if not analytics:
            return
        for library in self.collection.libraries:
            analytics.collect_event(
                library, self, event_name, as_of,
                old_value=old_value, new_value=new_value
            )

    def update_availability_from_delta(self, event_type, event_date, delta, analytics=None):
        """Call update_availability based on a single change seen in the
        distributor data, rather than a complete snapshot of
        distributor information as of a certain time.

        This information is unlikely to be completely accurate, but it
        should suffice until more accurate information can be
        obtained.

        No CirculationEvent is created until `update_availability` is
        called.

        Events must be processed in chronological order. Any event
        that happened than `LicensePool.last_checked` is ignored, and
        calling this method will update `LicensePool.last_checked` to
        the time of the event.

        :param event_type: A CirculationEvent constant representing the
        type of change that was seen.

        :param event_date: A datetime corresponding to when the 
        change was seen.

        :param delta: The magnitude of the change that was seen.

        """
        ignore = False
        if event_date != CirculationEvent.NO_DATE and self.last_checked and event_date < self.last_checked:
            # This is an old event and its effect on availability has
            # already been taken into account.
            ignore = True

        elif self.last_checked and event_date == CirculationEvent.NO_DATE:
            # We have a history for this LicensePool and we don't know
            # where this event fits into that history. Ignore the
            # event.
            ignore = True

        if not ignore:
            (new_licenses_owned, new_licenses_available, 
             new_licenses_reserved, 
             new_patrons_in_hold_queue) = self._calculate_change_from_one_event(
                 event_type, delta
             )

            changes_made = self.update_availability(
                new_licenses_owned, new_licenses_available, 
                new_licenses_reserved, new_patrons_in_hold_queue,
                analytics=analytics, as_of=event_date
            )
        if ignore or not changes_made:
            # Even if the event was ignored or didn't actually change
            # availability, we want to record receipt of the event
            # in the analytics.
            self.collect_analytics_event(
                analytics, event_type, event_date, 0, 0
            )

    def _calculate_change_from_one_event(self, type, delta):
        new_licenses_owned = self.licenses_owned
        new_licenses_available = self.licenses_available
        new_licenses_reserved = self.licenses_reserved
        new_patrons_in_hold_queue = self.patrons_in_hold_queue

        def deduct(value):
            # It's impossible for any of these numbers to be
            # negative.
            return max(value-delta, 0)

        CE = CirculationEvent
        added = False
        if type == CE.DISTRIBUTOR_HOLD_PLACE:
            new_patrons_in_hold_queue += delta
            if new_licenses_available:
                # If someone has put a book on hold, it must not be
                # immediately available.
                new_licenses_available = 0
        elif type == CE.DISTRIBUTOR_HOLD_RELEASE:
            new_patrons_in_hold_queue = deduct(new_patrons_in_hold_queue)
        elif type == CE.DISTRIBUTOR_CHECKIN:
            if self.patrons_in_hold_queue == 0:
                new_licenses_available += delta
            else:
                # When there are patrons in the hold queue, checking
                # in a single book does not make new licenses
                # available.  Checking in more books than there are
                # patrons in the hold queue _does_ make books
                # available.  However, in neither case do patrons
                # leave the hold queue. That will happen in the near
                # future as DISTRIBUTOR_AVAILABILITY_NOTIFICATION events 
                # are sent out.
                if delta > new_patrons_in_hold_queue:
                    new_licenses_available += (delta-new_patrons_in_hold_queue)
        elif type == CE.DISTRIBUTOR_CHECKOUT:
            if new_licenses_available == 0:
                # The only way to borrow books while there are no
                # licenses available is to borrow reserved copies.
                new_licenses_reserved = deduct(new_licenses_reserved)
            else:
                # We don't know whether this checkout came from
                # licenses available or from a lingering reserved
                # copy, but in most cases it came from licenses
                # available.
                new_licenses_available = deduct(new_licenses_available)
        elif type == CE.DISTRIBUTOR_LICENSE_ADD:
            new_licenses_owned += delta
            # Newly added licenses start out as available, unless there
            # are patrons in the holds queue.
            if new_patrons_in_hold_queue == 0:
                new_licenses_available += delta
        elif type == CE.DISTRIBUTOR_LICENSE_REMOVE:
            new_licenses_owned = deduct(new_licenses_owned)
            # We can't say whether or not the removed licenses should
            # be deducted from the list of available licenses, because they
            # might already be checked out.
        elif type == CE.DISTRIBUTOR_AVAILABILITY_NOTIFY:
            new_patrons_in_hold_queue = deduct(new_patrons_in_hold_queue)
            new_licenses_reserved += delta
        if new_licenses_owned < new_licenses_available:
            # It's impossible to have more licenses available than
            # owned. We don't know whether this means there are some
            # extra licenses we never heard about, or whether some
            # licenses expired without us being notified, but the
            # latter is more likely.
            new_licenses_available = new_licenses_owned

        return (new_licenses_owned, new_licenses_available, 
                new_licenses_reserved, new_patrons_in_hold_queue)

    def circulation_changelog(self, old_licenses_owned, old_licenses_available,
                              old_licenses_reserved, old_patrons_in_hold_queue):
        """Generate a log message describing a change to the circulation.

        :return: a 2-tuple (message, args) suitable for passing into 
        logging.info or a similar method
        """
        edition = self.presentation_edition
        message = u'CHANGED '
        args = []
        if self.identifier:
            identifier_template = '%s/%s'
            identifier_args = [self.identifier.type, self.identifier.identifier]
        else:
            identifier_template = '%s'
            identifier_args = [self.identifier]
        if edition:
            message += u'%s "%s" %s (' + identifier_template + ')'
            args.extend([edition.medium, 
                         edition.title or "[NO TITLE]",
                         edition.author or "[NO AUTHOR]"]
                    )
            args.extend(identifier_args)
        else:
            message += identifier_template
            args.extend(identifier_args)

        def _part(message, args, string, old_value, new_value):
            if old_value != new_value:
                args.extend([string, old_value, new_value])
                message += ' %s: %s=>%s'
            return message, args

        message, args = _part(
            message, args, "OWN", old_licenses_owned, self.licenses_owned
        )
        
        message, args = _part(
            message, args, "AVAIL", old_licenses_available, 
            self.licenses_available
        )

        message, args = _part(
            message, args, "RSRV", old_licenses_reserved, 
            self.licenses_reserved
        )

        message, args =_part(
            message, args, "HOLD", old_patrons_in_hold_queue, 
            self.patrons_in_hold_queue
        )
        return message, tuple(args)

    def loan_to(self, patron, start=None, end=None, fulfillment=None, external_identifier=None):
        _db = Session.object_session(patron)
        kwargs = dict(start=start or datetime.datetime.utcnow(),
                      end=end)
        loan, is_new = get_one_or_create(
            _db, Loan, patron=patron, license_pool=self, 
            create_method_kwargs=kwargs)
        if fulfillment:
            loan.fulfillment = fulfillment
        if external_identifier:
            loan.external_identifier = external_identifier
        return loan, is_new

    def on_hold_to(self, patron, start=None, end=None, position=None):
        _db = Session.object_session(patron)
        if not patron.library.allow_holds:
            raise PolicyException("Holds are disabled for this library.")
        start = start or datetime.datetime.utcnow()
        hold, new = get_one_or_create(
            _db, Hold, patron=patron, license_pool=self)
        hold.update(start, end, position)
        return hold, new

    @classmethod
    def consolidate_works(cls, _db, calculate_work_even_if_no_author=False,
                          batch_size=10):
        """Assign a (possibly new) Work to every unassigned LicensePool."""
        a = 0
        lps = cls.with_no_work(_db)
        logging.info(
            "Assigning Works to %d LicensePools with no Work.", len(lps)
        )
        for unassigned in lps:
            etext, new = unassigned.calculate_work(
                even_if_no_author=calculate_work_even_if_no_author)
            if not etext:
                # We could not create a work for this LicensePool,
                # most likely because it does not yet have any
                # associated Edition.
                continue
            a += 1
            logging.info("When consolidating works, created %r", etext)
            if a and not a % batch_size:
                _db.commit()
        _db.commit()


    def calculate_work(
        self, even_if_no_author=False, known_edition=None, exclude_search=False
    ):
        """Find or create a Work for this LicensePool.

        A pool that is not open-access will always have its own
        Work. Open-access LicensePools will be grouped together with
        other open-access LicensePools based on the permanent work ID
        of the LicensePool's presentation edition.

        :param even_if_no_author: Ordinarily this method will refuse
        to create a Work for a LicensePool whose Edition has no title
        or author. But sometimes a book just has no known author. If
        that's really the case, pass in even_if_no_author=True and the
        Work will be created.

        TODO: I think known_edition is mostly useless. We should
        either remove it or replace it with a boolean that stops us
        from calling set_presentation_edition() and assumes we've
        already done that work.
        """
        if not self.identifier:
            # A LicensePool with no Identifier should never have a Work.
            self.work = None
            return None, False
       
        if known_edition:
            presentation_edition = known_edition
        else:
            self.set_presentation_edition()
            presentation_edition = self.presentation_edition
            
        if presentation_edition:
            if self not in presentation_edition.is_presentation_for:
                raise ValueError(
                    "Alleged presentation edition is not the presentation edition for the license pool for which work is being calculated!"
                )
                    
        logging.info("Calculating work for %r", presentation_edition)
        if not presentation_edition:
            # We don't have any information about the identifier
            # associated with this LicensePool, so we can't create a work.
            logging.warn("NO EDITION for %s, cowardly refusing to create work.",
                     self.identifier)

            # If there was a work associated with this LicensePool,
            # it was by mistake. Remove it.
            self.work = None
            return None, False

        if not presentation_edition.title or not presentation_edition.author:
            presentation_edition.calculate_presentation()

        if not presentation_edition.title:
            if presentation_edition.work:
                logging.warn(
                    "Edition %r has no title but has a Work assigned. This will not stand.", presentation_edition
                )
            else:
                logging.info("Edition %r has no title and it will not get a Work.", presentation_edition)
            self.work = None
            self.work_id = None
            return None, False

        if (not presentation_edition.work
            and presentation_edition.author in (None, Edition.UNKNOWN_AUTHOR)
            and not even_if_no_author
        ):
            logging.warn(
                "Edition %r has no author, not assigning Work to Edition.", 
                presentation_edition
            )
            # If there was a work associated with this LicensePool,
            # it was by mistake. Remove it.
            self.work = None
            self.work_id = None
            return None, False

        presentation_edition.calculate_permanent_work_id()

        _db = Session.object_session(self)
        work = None
        is_new = False
        licensepools_changed = False
        if self.open_access and presentation_edition.permanent_work_id:
            # This is an open-access book. Use the Work for all
            # open-access books associated with this book's permanent
            # work ID.
            #
            # If the dataset is in an inconsistent state, calling
            # Work.open_access_for_permanent_work_id may result in works being
            # merged.
            work, is_new = Work.open_access_for_permanent_work_id(
                _db, presentation_edition.permanent_work_id,
                presentation_edition.medium
            )

            # Run a sanity check to make sure every LicensePool
            # associated with this Work actually belongs there. This
            # may result in new Works being created.
            #
            # This could go into Work.for_permanent_work_id, but that
            # could conceivably lead to an infinite loop, or at least
            # a very long recursive call, so I've put it here.
            work.make_exclusive_open_access_for_permanent_work_id(
                presentation_edition.permanent_work_id, 
                presentation_edition.medium
            )
            self.work = work
            licensepools_changed = True

        # All LicensePools with a given Identifier must share a work.
        existing_works = set([x.work for x in self.identifier.licensed_through])
        if len(existing_works) > 1:
            logging.warn(
                "LicensePools for %r have more than one Work between them. Removing them all and starting over."
            )
            for lp in self.identifier.licensed_through:
                lp.work = None
                if lp.presentation_edition:
                    lp.presentation_edition.work = None
        else:
            # There is a consensus Work for this Identifier.
            [self.work] = existing_works

        if self.work:
            # This pool is already associated with a Work. Use that
            # Work.
            work = self.work
        elif presentation_edition.work:
            # This pool's presentation edition is already associated with
            # a Work. Use that Work.
            work = presentation_edition.work
            self.work = work

        if work:
            # There is already a Work associated with this LicensePool,
            # but we need to run a sanity check because occasionally
            # LicensePools get mis-grouped due to bugs.
            #
            # A commercially-licensed book should have a Work to
            # itself. All other LicensePools need to be kicked out and
            # associated with some other work.
            #
            # This won't cause an infinite recursion because we're
            # setting pool.work to None before calling
            # pool.calculate_work(), and the recursive call only
            # happens if self.work is set.
            for pool in list(work.license_pools):
                if pool is self:
                    continue
                if not (self.open_access and pool.open_access):
                    pool.work = None
                    pool.calculate_work(exclude_search=exclude_search)
                    licensepools_changed = True

        else:
            # There is no better choice than creating a brand new Work.
            is_new = True
            logging.info(
                "Creating a new work for %r" % presentation_edition.title
            )
            work = Work()
            _db = Session.object_session(self)
            _db.add(work)
            flush(_db)
            licensepools_changed = True

        # Associate this LicensePool and its Edition with the work we
        # chose or created.
        if not self in work.license_pools:
            work.license_pools.append(self)
            licensepools_changed = True

        # Recalculate the display information for the Work, since the
        # associated LicensePools have changed, which may have caused
        # the Work's presentation Edition to change.
        #
        # TODO: In theory we can speed things up by only calling
        # calculate_presentation if licensepools_changed is
        # True. However, some bits of other code call calculate_work()
        # under the assumption that it always calls
        # calculate_presentation(), so we'd need to evaluate those
        # call points first.
        work.calculate_presentation(exclude_search=exclude_search)

        # Ensure that all LicensePools with this Identifier share
        # the same Work. (We may have wiped out their .work earlier
        # in this method.)
        for lp in self.identifier.licensed_through:
            lp.work = work
        
        if is_new:
            logging.info("Created a new work: %r", work)

        # All done!
        return work, is_new


    @property
    def open_access_links(self):
        """Yield all open-access Resources for this LicensePool."""

        open_access = Hyperlink.OPEN_ACCESS_DOWNLOAD
        _db = Session.object_session(self)
        if not self.identifier:
            return
        q = Identifier.resources_for_identifier_ids(
            _db, [self.identifier.id], open_access
        )
        for resource in q:
            yield resource

    @property
    def open_access_download_url(self):
        """Alias for best_open_access_link.

        If _open_access_download_url is currently None, this will set
        to a good value if possible.
        """
        return self.best_open_access_link
        
    @property
    def best_open_access_link(self):
        """Find the best open-access link for this LicensePool.

        Cache it so that the next access will be faster.
        """
        if not self.open_access:
            return None
        if not self._open_access_download_url:
            url = None
            resource = self.best_open_access_resource
            if resource and resource.representation:
                url = resource.representation.mirror_url
            self._open_access_download_url = url
        return self._open_access_download_url

    @property
    def best_open_access_resource(self):
        """Determine the best open-access Resource currently provided by this 
        LicensePool.
        """
        best = None
        best_priority = -1
        for resource in self.open_access_links:
            if not any(
                    [resource.representation and
                     resource.representation.media_type and
                     resource.representation.media_type.startswith(x) 
                     for x in Representation.SUPPORTED_BOOK_MEDIA_TYPES]):
                # This representation is not in a media type we 
                # support. We can't serve it, so we won't consider it.
                continue
                
            data_source_priority = self.open_access_source_priority
            if not best or data_source_priority > best_priority:
                # Something is better than nothing.
                best = resource
                best_priority = data_source_priority
                continue

            if (best.data_source.name==DataSource.GUTENBERG
                and resource.data_source.name==DataSource.GUTENBERG
                and 'noimages' in best.representation.mirror_url
                and not 'noimages' in resource.representation.mirror_url):
                # A Project Gutenberg-ism: an epub without 'noimages'
                # in the filename is better than an epub with
                # 'noimages' in the filename.
                best = resource
                best_priority = data_source_priority
                continue

        return best

    @property
    def best_license_link(self):
        """Find the best available licensing link for the work associated
        with this LicensePool.

        # TODO: This needs work and may not be necessary anymore.
        """
        edition = self.edition
        if not edition:
            return self, None
        link = edition.best_open_access_link
        if link:
            return self, link

        # Either this work is not open-access, or there was no epub
        # link associated with it.
        work = self.work
        for pool in work.license_pools:
            edition = pool.edition
            link = edition.best_open_access_link
            if link:
                return pool, link
        return self, None

    def set_delivery_mechanism(self, *args, **kwargs):
        """Ensure that this LicensePool (and any other LicensePools for the same
        book) have a LicensePoolDeliveryMechanism for this media type,
        DRM scheme, rights status, and resource.
        """
        return LicensePoolDeliveryMechanism.set(
            self.data_source, self.identifier, *args, **kwargs
        )

Index("ix_licensepools_data_source_id_identifier_id_collection_id", LicensePool.collection_id, LicensePool.data_source_id, LicensePool.identifier_id, unique=True)
