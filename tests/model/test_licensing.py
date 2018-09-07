# encoding: utf-8
import base64
import datetime
from nose.tools import (
    assert_raises,
    assert_raises_regexp,
    assert_not_equal,
    eq_,
    set_trace,
)
from sqlalchemy.exc import (
    IntegrityError,
)
import core.model
from core.model.licensing import (
    Collection,
    LicensePool,
    LicensePoolDeliveryMechanism,
    DeliveryMechanism,
    RightsStatus,
)
from .. import DatabaseTest
from mock_analytics_provider import MockAnalyticsProvider

class TestCollection(DatabaseTest):

    def setup(self):
        super(TestCollection, self).setup()
        self.collection = self._collection(
            name="test collection", protocol=ExternalIntegration.OVERDRIVE
        )

    def test_by_name_and_protocol(self):
        name = "A name"
        protocol = ExternalIntegration.OVERDRIVE
        key = (name, protocol)

        # Cache is empty.
        eq_(HasFullTableCache.RESET, Collection._cache)

        collection1, is_new = Collection.by_name_and_protocol(
            self._db, name, ExternalIntegration.OVERDRIVE
        )
        eq_(True, is_new)

        # Cache was populated and then reset because we created a new
        # Collection.
        eq_(HasFullTableCache.RESET, Collection._cache)

        collection2, is_new = Collection.by_name_and_protocol(
            self._db, name, ExternalIntegration.OVERDRIVE
        )
        eq_(collection1, collection2)
        eq_(False, is_new)

        # This time the cache was not reset after being populated.
        eq_(collection1, Collection._cache[key])

        # You'll get an exception if you look up an existing name
        # but the protocol doesn't match.
        assert_raises_regexp(
            ValueError,
            'Collection "A name" does not use protocol "Bibliotheca".',
            Collection.by_name_and_protocol,
            self._db, name, ExternalIntegration.BIBLIOTHECA
        )

    def test_by_protocol(self):
        """Verify the ability to find all collections that implement
        a certain protocol.
        """
        overdrive = ExternalIntegration.OVERDRIVE
        bibliotheca = ExternalIntegration.BIBLIOTHECA
        c1 = self._collection(self._str, protocol=overdrive)
        c1.parent = self.collection
        c2 = self._collection(self._str, protocol=bibliotheca)
        eq_(set([self.collection, c1]),
            set(Collection.by_protocol(self._db, overdrive).all()))
        eq_(([c2]),
            Collection.by_protocol(self._db, bibliotheca).all())
        eq_(set([self.collection, c1, c2]),
            set(Collection.by_protocol(self._db, None).all()))

    def test_by_datasource(self):
        """Collections can be found by their associated DataSource"""
        c1 = self._collection(data_source_name=DataSource.GUTENBERG)
        c2 = self._collection(data_source_name=DataSource.OVERDRIVE)

        # Using the DataSource name
        eq_(set([c1]),
            set(Collection.by_datasource(self._db, DataSource.GUTENBERG).all()))

        # Using the DataSource itself
        overdrive = DataSource.lookup(self._db, DataSource.OVERDRIVE)
        eq_(set([c2]),
            set(Collection.by_datasource(self._db, overdrive).all()))

    def test_parents(self):
        # Collections can return all their parents recursively.
        c1 = self._collection()
        eq_([], list(c1.parents))

        c2 = self._collection()
        c2.parent_id = c1.id
        eq_([c1], list(c2.parents))

        c3 = self._collection()
        c3.parent_id = c2.id
        eq_([c2, c1], list(c3.parents))

    def test_create_external_integration(self):
        # A newly created Collection has no associated ExternalIntegration.
        collection, ignore = get_one_or_create(
            self._db, Collection, name=self._str
        )
        eq_(None, collection.external_integration_id)
        assert_raises_regexp(
            ValueError,
            "No known external integration for collection",
            getattr, collection, 'external_integration'
        )

        # We can create one with create_external_integration().
        overdrive = ExternalIntegration.OVERDRIVE
        integration = collection.create_external_integration(protocol=overdrive)
        eq_(integration.id, collection.external_integration_id)
        eq_(overdrive, integration.protocol)

        # If we call create_external_integration() again we get the same
        # ExternalIntegration as before.
        integration2 = collection.create_external_integration(protocol=overdrive)
        eq_(integration, integration2)


        # If we try to initialize an ExternalIntegration with a different
        # protocol, we get an error.
        assert_raises_regexp(
            ValueError,
            "Located ExternalIntegration, but its protocol \(Overdrive\) does not match desired protocol \(blah\).",
            collection.create_external_integration,
            protocol="blah"
        )

    def test_change_protocol(self):
        overdrive = ExternalIntegration.OVERDRIVE
        bibliotheca = ExternalIntegration.BIBLIOTHECA

        # Create a parent and a child collection, both with
        # protocol=Overdrive.
        child = self._collection(self._str, protocol=overdrive)
        child.parent = self.collection

        # We can't change the child's protocol to a value that contradicts
        # the parent's protocol.
        child.protocol = overdrive
        def set_child_protocol():
            child.protocol = bibliotheca
        assert_raises_regexp(
            ValueError,
            "Proposed new protocol \(Bibliotheca\) contradicts parent collection's protocol \(Overdrive\).",
            set_child_protocol
        )

        # If we change the parent's protocol, the children are
        # automatically updated.
        self.collection.protocol = bibliotheca
        eq_(bibliotheca, child.protocol)

    def test_data_source(self):
        opds = self._collection()
        bibliotheca = self._collection(protocol=ExternalIntegration.BIBLIOTHECA)

        # The rote data_source is returned for the obvious collection.
        eq_(DataSource.BIBLIOTHECA, bibliotheca.data_source.name)

        # The less obvious OPDS collection doesn't have a DataSource.
        eq_(None, opds.data_source)

        # Trying to change the Bibliotheca collection's data_source does nothing.
        bibliotheca.data_source = DataSource.AXIS_360
        eq_(DataSource.BIBLIOTHECA, bibliotheca.data_source.name)

        # Trying to change the opds collection's data_source is fine.
        opds.data_source = DataSource.PLYMPTON
        eq_(DataSource.PLYMPTON, opds.data_source.name)

        # Resetting it to something else is fine.
        opds.data_source = DataSource.OA_CONTENT_SERVER
        eq_(DataSource.OA_CONTENT_SERVER, opds.data_source.name)

        # Resetting it to None is fine.
        opds.data_source = None
        eq_(None, opds.data_source)

    def test_default_loan_period(self):
        library = self._default_library
        library.collections.append(self.collection)

        ebook = Edition.BOOK_MEDIUM
        audio = Edition.AUDIO_MEDIUM

        # The default when no value is set.
        eq_(
            Collection.STANDARD_DEFAULT_LOAN_PERIOD,
            self.collection.default_loan_period(library, ebook)
        )

        eq_(
            Collection.STANDARD_DEFAULT_LOAN_PERIOD,
            self.collection.default_loan_period(library, audio)
        )

        # Set a value, and it's used.
        self.collection.default_loan_period_setting(library, ebook).value = 604
        eq_(604, self.collection.default_loan_period(library))
        eq_(
            Collection.STANDARD_DEFAULT_LOAN_PERIOD,
            self.collection.default_loan_period(library, audio)
        )

        self.collection.default_loan_period_setting(library, audio).value = 606
        eq_(606, self.collection.default_loan_period(library, audio))

        # Given an integration client rather than a library, use
        # a sitewide integration setting rather than a library-specific
        # setting.
        client = self._integration_client()

        # The default when no value is set.
        eq_(
            Collection.STANDARD_DEFAULT_LOAN_PERIOD,
            self.collection.default_loan_period(client, ebook)
        )

        eq_(
            Collection.STANDARD_DEFAULT_LOAN_PERIOD,
            self.collection.default_loan_period(client, audio)
        )

        # Set a value, and it's used.
        self.collection.default_loan_period_setting(client, ebook).value = 347
        eq_(347, self.collection.default_loan_period(client))
        eq_(
            Collection.STANDARD_DEFAULT_LOAN_PERIOD,
            self.collection.default_loan_period(client, audio)
        )

        self.collection.default_loan_period_setting(client, audio).value = 349
        eq_(349, self.collection.default_loan_period(client, audio))

        # The same value is used for other clients.
        client2 = self._integration_client()
        eq_(347, self.collection.default_loan_period(client))
        eq_(349, self.collection.default_loan_period(client, audio))

    def test_default_reservation_period(self):
        library = self._default_library
        # The default when no value is set.
        eq_(
            Collection.STANDARD_DEFAULT_RESERVATION_PERIOD,
            self.collection.default_reservation_period
        )

        # Set a value, and it's used.
        self.collection.default_reservation_period = 601
        eq_(601, self.collection.default_reservation_period)

        # The underlying value is controlled by a ConfigurationSetting.
        self.collection.external_integration.setting(
            Collection.DEFAULT_RESERVATION_PERIOD_KEY
        ).value = 954
        eq_(954, self.collection.default_reservation_period)

    def test_explain(self):
        """Test that Collection.explain gives all relevant information
        about a Collection.
        """
        library = self._default_library
        library.name="The only library"
        library.short_name = "only one"
        library.collections.append(self.collection)

        self.collection.external_account_id = "id"
        self.collection.external_integration.url = "url"
        self.collection.external_integration.username = "username"
        self.collection.external_integration.password = "password"
        setting = self.collection.external_integration.set_setting("setting", "value")

        data = self.collection.explain()
        eq_(['Name: "test collection"',
             'Protocol: "Overdrive"',
             'Used by library: "only one"',
             'External account ID: "id"',
             'Setting "setting": "value"',
             'Setting "url": "url"',
             'Setting "username": "username"',
        ],
            data
        )

        with_password = self.collection.explain(include_secrets=True)
        assert 'Setting "password": "password"' in with_password

        # If the collection is the child of another collection,
        # its parent is mentioned.
        child = Collection(
            name="Child", parent=self.collection, external_account_id="id2"
        )
        child.create_external_integration(
            protocol=ExternalIntegration.OVERDRIVE
        )
        data = child.explain()
        eq_(['Name: "Child"',
             'Parent: test collection',
             'Protocol: "Overdrive"',
             'External account ID: "id2"'],
            data
        )

    def test_metadata_identifier(self):
        # If the collection doesn't have its unique identifier, an error
        # is raised.
        assert_raises(ValueError, getattr, self.collection, 'metadata_identifier')

        def build_expected(protocol, unique_id):
            encoded = [base64.b64encode(unicode(value), '-_')
                       for value in [protocol, unique_id]]
            return base64.b64encode(':'.join(encoded), '-_')

        # With a unique identifier, we get back the expected identifier.
        self.collection.external_account_id = 'id'
        expected = build_expected(ExternalIntegration.OVERDRIVE, 'id')
        eq_(expected, self.collection.metadata_identifier)

        # If there's a parent, its unique id is incorporated into the result.
        child = self._collection(
            name="Child", protocol=ExternalIntegration.OPDS_IMPORT,
            external_account_id=self._url
        )
        child.parent = self.collection
        expected = build_expected(ExternalIntegration.OPDS_IMPORT, 'id+%s' % child.external_account_id)
        eq_(expected, child.metadata_identifier)

        # If it's an OPDS_IMPORT collection with a url external_account_id,
        # closing '/' marks are removed.
        opds = self._collection(
            name='OPDS', protocol=ExternalIntegration.OPDS_IMPORT,
            external_account_id=(self._url+'/')
        )
        expected = build_expected(ExternalIntegration.OPDS_IMPORT, opds.external_account_id[:-1])
        eq_(expected, opds.metadata_identifier)

    def test_from_metadata_identifier(self):
        # If a mirrored collection doesn't exist, it is created.
        self.collection.external_account_id = 'id'
        mirror_collection, is_new = Collection.from_metadata_identifier(
            self._db, self.collection.metadata_identifier
        )
        eq_(True, is_new)
        eq_(self.collection.metadata_identifier, mirror_collection.name)
        eq_(self.collection.protocol, mirror_collection.protocol)
        # Because this isn't an OPDS collection, no account details are held.
        eq_(None, mirror_collection.external_account_id)

        # If the mirrored collection already exists, it is returned.
        collection = self._collection(external_account_id=self._url)
        mirror_collection = create(
            self._db, Collection,
            name=collection.metadata_identifier,
        )[0]
        mirror_collection.create_external_integration(collection.protocol)
        # Confirm that there's no external_account_id and no DataSource.
        eq_(None, mirror_collection.external_account_id)
        eq_(None, mirror_collection.data_source)

        source = DataSource.lookup(self._db, DataSource.OA_CONTENT_SERVER)
        result, is_new = Collection.from_metadata_identifier(
            self._db, collection.metadata_identifier, data_source=source
        )
        eq_(False, is_new)
        eq_(mirror_collection, result)
        # The external_account_id and data_source have been set now.
        eq_(collection.external_account_id, mirror_collection.external_account_id)
        eq_(source, mirror_collection.data_source)

    def test_catalog_identifier(self):
        """#catalog_identifier associates an identifier with the catalog"""
        identifier = self._identifier()
        self.collection.catalog_identifier(identifier)

        eq_(1, len(self.collection.catalog))
        eq_(identifier, self.collection.catalog[0])

    def test_catalog_identifiers(self):
        """#catalog_identifier associates multiple identifiers with a catalog"""
        i1 = self._identifier()
        i2 = self._identifier()
        i3 = self._identifier()

        # One of the identifiers is already in the catalog.
        self.collection.catalog_identifier(i3)

        self.collection.catalog_identifiers([i1, i2, i3])

        # Now all three identifiers are in the catalog.
        assert sorted([i1, i2, i3]) == sorted(self.collection.catalog)

    def test_unresolved_catalog(self):
        # A regular schmegular identifier: untouched, pure.
        pure_id = self._identifier()

        # A 'resolved' identifier that doesn't have a work yet.
        # (This isn't supposed to happen, but jic.)
        source = DataSource.lookup(self._db, DataSource.GUTENBERG)
        operation = 'test-thyself'
        resolved_id = self._identifier()
        self._coverage_record(
            resolved_id, source, operation=operation,
            status=CoverageRecord.SUCCESS
        )

        # An unresolved identifier--we tried to resolve it, but
        # it all fell apart.
        unresolved_id = self._identifier()
        self._coverage_record(
            unresolved_id, source, operation=operation,
            status=CoverageRecord.TRANSIENT_FAILURE
        )

        # An identifier with a Work already.
        id_with_work = self._work().presentation_edition.primary_identifier


        self.collection.catalog_identifiers([
            pure_id, resolved_id, unresolved_id, id_with_work
        ])

        result = self.collection.unresolved_catalog(
            self._db, source.name, operation
        )

        # Only the failing identifier is in the query.
        eq_([unresolved_id], result.all())

    def test_works_updated_since(self):
        w1 = self._work(with_license_pool=True)
        w2 = self._work(with_license_pool=True)
        w3 = self._work(with_license_pool=True)

        # An empty catalog returns nothing.
        timestamp = datetime.datetime.utcnow()
        eq_([], self.collection.works_updated_since(self._db, timestamp).all())

        self.collection.catalog_identifier(w1.license_pools[0].identifier)
        self.collection.catalog_identifier(w2.license_pools[0].identifier)

        # This Work is catalogued in another catalog and will never show up.
        collection2 = self._collection()
        in_other_catalog = self._work(
            with_license_pool=True, collection=collection2
        )
        collection2.catalog_identifier(
            in_other_catalog.license_pools[0].identifier
        )

        # When no timestamp is passed, all works in the catalog are returned.
        # in order of their WorkCoverageRecord timestamp.
        t1, t2 = self.collection.works_updated_since(self._db, None).all()
        eq_(w1, t1[0])
        eq_(w2, t2[0])

        # The return value is a sequence of 5-tuples, each containing
        # (Work, LicensePool, Identifier, WorkCoverageRecord,
        # CollectionIdentifier). This gives the caller all the information
        # necessary to understand the path by which a given Work belongs to
        # a given Collection.
        _w1, lp1, i1 = t1
        [pool] = w1.license_pools
        eq_(pool, lp1)
        eq_(pool.identifier, i1)

        # When a timestamp is passed, only works that have been updated
        # since then will be returned
        [w1_coverage_record] = [
            c for c in w1.coverage_records
            if c.operation == WorkCoverageRecord.GENERATE_OPDS_OPERATION
        ]
        w1_coverage_record.timestamp = datetime.datetime.utcnow()
        eq_([w1], [x[0] for x in self.collection.works_updated_since(self._db, timestamp)])

    def test_isbns_updated_since(self):
        i1 = self._identifier(identifier_type=Identifier.ISBN, foreign_id=self._isbn)
        i2 = self._identifier(identifier_type=Identifier.ISBN, foreign_id=self._isbn)
        i3 = self._identifier(identifier_type=Identifier.ISBN, foreign_id=self._isbn)
        i4 = self._identifier(identifier_type=Identifier.ISBN, foreign_id=self._isbn)

        timestamp = datetime.datetime.utcnow()

        # An empty catalog returns nothing..
        eq_([], self.collection.isbns_updated_since(self._db, None).all())

        # Give the ISBNs some coverage.
        content_cafe = DataSource.lookup(self._db, DataSource.CONTENT_CAFE)
        for isbn in [i2, i3, i1]:
            self._coverage_record(isbn, content_cafe)

        # Give one ISBN more than one coverage record.
        oclc = DataSource.lookup(self._db, DataSource.OCLC)
        i1_oclc_record = self._coverage_record(i1, oclc)

        def assert_isbns(expected, result_query):
            results = [r[0] for r in result_query]
            eq_(expected, results)

        # When no timestamp is given, all ISBNs in the catalog are returned,
        # in order of their CoverageRecord timestamp.
        self.collection.catalog_identifiers([i1, i2])
        updated_isbns = self.collection.isbns_updated_since(self._db, None).all()
        assert_isbns([i2, i1], updated_isbns)

        # That CoverageRecord timestamp is also returned.
        i1_timestamp = updated_isbns[1][1]
        assert isinstance(i1_timestamp, datetime.datetime)
        eq_(i1_oclc_record.timestamp, i1_timestamp)

        # When a timestamp is passed, only works that have been updated since
        # then will be returned.
        timestamp = datetime.datetime.utcnow()
        i1.coverage_records[0].timestamp = datetime.datetime.utcnow()
        updated_isbns = self.collection.isbns_updated_since(self._db, timestamp)
        assert_isbns([i1], updated_isbns)

        # Prepare an ISBN associated with a Work.
        work = self._work(with_license_pool=True)
        work.license_pools[0].identifier = i2
        i2.coverage_records[0].timestamp = datetime.datetime.utcnow()

        # ISBNs that have a Work will be ignored.
        updated_isbns = self.collection.isbns_updated_since(self._db, timestamp)
        assert_isbns([i1], updated_isbns)

    def test_custom_lists(self):
        # A Collection can be associated with one or more CustomLists.
        list1, ignore = get_one_or_create(self._db, CustomList, name=self._str)
        list2, ignore = get_one_or_create(self._db, CustomList, name=self._str)
        self.collection.customlists = [list1, list2]
        eq_(0, len(list1.entries))
        eq_(0, len(list2.entries))

        # When a new pool is added to the collection and its presentation edition is
        # calculated for the first time, it's automatically added to the lists.
        work = self._work(collection=self.collection, with_license_pool=True)
        eq_(1, len(list1.entries))
        eq_(1, len(list2.entries))
        eq_(work, list1.entries[0].work)
        eq_(work, list2.entries[0].work)

        # Now remove it from one of the lists. If its presentation edition changes
        # again or its pool changes works, it's not added back.
        self._db.delete(list1.entries[0])
        self._db.commit()
        eq_(0, len(list1.entries))
        eq_(1, len(list2.entries))

        pool = work.license_pools[0]
        identifier = pool.identifier
        staff_data_source = DataSource.lookup(self._db, DataSource.LIBRARY_STAFF)
        staff_edition, ignore = Edition.for_foreign_id(
            self._db, staff_data_source,
            identifier.type, identifier.identifier)

        staff_edition.title = self._str
        work.calculate_presentation()
        eq_(0, len(list1.entries))
        eq_(1, len(list2.entries))

        new_work = self._work(collection=self.collection)
        pool.work = new_work
        eq_(0, len(list1.entries))
        eq_(1, len(list2.entries))
class TestLicensePool(DatabaseTest):

    def test_for_foreign_id(self):
        """Verify we can get a LicensePool for a data source, an
        appropriate work identifier, and a Collection."""
        now = datetime.datetime.utcnow()
        pool, was_new = LicensePool.for_foreign_id(
            self._db, DataSource.GUTENBERG, Identifier.GUTENBERG_ID, "541",
            collection=self._collection()
        )
        assert (pool.availability_time - now).total_seconds() < 2
        eq_(True, was_new)
        eq_(DataSource.GUTENBERG, pool.data_source.name)
        eq_(Identifier.GUTENBERG_ID, pool.identifier.type)
        eq_("541", pool.identifier.identifier)
        eq_(0, pool.licenses_owned)
        eq_(0, pool.licenses_available)
        eq_(0, pool.licenses_reserved)
        eq_(0, pool.patrons_in_hold_queue)

    def test_for_foreign_id_fails_when_no_collection_provided(self):
        """We cannot create a LicensePool that is not associated
        with some Collection.
        """
        assert_raises(
            CollectionMissing,
            LicensePool.for_foreign_id,
            self._db, DataSource.GUTENBERG, Identifier.GUTENBERG_ID, "541",
            collection=None
        )

    def test_no_license_pool_for_non_primary_identifier(self):
        """Overdrive offers licenses, but to get an Overdrive license pool for
        a book you must identify the book by Overdrive's primary
        identifier, not some other kind of identifier.
        """
        collection = self._collection()
        assert_raises_regexp(
            ValueError,
            "License pools for data source 'Overdrive' are keyed to identifier type 'Overdrive ID' \(not 'ISBN', which was provided\)",
            LicensePool.for_foreign_id,
            self._db, DataSource.OVERDRIVE, Identifier.ISBN, "{1-2-3}",
            collection=collection
        )

    def test_licensepools_for_same_identifier_have_same_presentation_edition(self):
        """Two LicensePools for the same Identifier will get the same
        presentation edition.
        """
        identifier = self._identifier()
        edition1, pool1 = self._edition(
            with_license_pool=True, data_source_name=DataSource.GUTENBERG,
            identifier_type=identifier.type, identifier_id=identifier.identifier
        )
        edition2, pool2 = self._edition(
            with_license_pool=True, data_source_name=DataSource.UNGLUE_IT,
            identifier_type=identifier.type, identifier_id=identifier.identifier
        )
        pool1.set_presentation_edition()
        pool2.set_presentation_edition()
        eq_(pool1.presentation_edition, pool2.presentation_edition)

    def test_collection_datasource_identifier_must_be_unique(self):
        """You can't have two LicensePools with the same Collection,
        DataSource, and Identifier.
        """
        data_source = DataSource.lookup(self._db, DataSource.GUTENBERG)
        identifier = self._identifier()
        collection = self._default_collection
        pool = create(
            self._db,
            LicensePool,
            data_source=data_source,
            identifier=identifier,
            collection=collection
        )

        assert_raises(
            IntegrityError,
            create,
            self._db,
            LicensePool,
            data_source=data_source,
            identifier=identifier,
            collection=collection
        )

    def test_with_no_work(self):
        p1, ignore = LicensePool.for_foreign_id(
            self._db, DataSource.GUTENBERG, Identifier.GUTENBERG_ID, "1",
            collection=self._default_collection
        )

        p2, ignore = LicensePool.for_foreign_id(
            self._db, DataSource.OVERDRIVE, Identifier.OVERDRIVE_ID, "2",
            collection=self._default_collection
        )

        work = self._work(title="Foo")
        p1.work = work

        assert p1 in work.license_pools

        eq_([p2], LicensePool.with_no_work(self._db))

    def test_update_availability(self):
        work = self._work(with_license_pool=True)
        work.last_update_time = None

        [pool] = work.license_pools
        pool.update_availability(30, 20, 2, 0)
        eq_(30, pool.licenses_owned)
        eq_(20, pool.licenses_available)
        eq_(2, pool.licenses_reserved)
        eq_(0, pool.patrons_in_hold_queue)

        # Updating availability also modified work.last_update_time.
        assert (datetime.datetime.utcnow() - work.last_update_time) < datetime.timedelta(seconds=2)

    def test_update_availability_triggers_analytics(self):
        work = self._work(with_license_pool=True)
        [pool] = work.license_pools
        provider = MockAnalyticsProvider()
        pool.update_availability(30, 20, 2, 0, analytics=provider)
        count = provider.count
        pool.update_availability(30, 21, 2, 0, analytics=provider)
        eq_(count + 1, provider.count)
        eq_(CirculationEvent.DISTRIBUTOR_CHECKIN, provider.event_type)
        pool.update_availability(30, 21, 2, 1, analytics=provider)
        eq_(count + 2, provider.count)
        eq_(CirculationEvent.DISTRIBUTOR_HOLD_PLACE, provider.event_type)

    def test_update_availability_does_nothing_if_given_no_data(self):
        """Passing an empty set of data into update_availability is
        a no-op.
        """

        # Set up a Work.
        work = self._work(with_license_pool=True)
        work.last_update_time = None

        # Set up a LicensePool.
        [pool] = work.license_pools
        pool.last_checked = None
        pool.licenses_owned = 10
        pool.licenses_available = 20
        pool.licenses_reserved = 30
        pool.patrons_in_hold_queue = 40

        # Pass empty values into update_availability.
        pool.update_availability(None, None, None, None)

        # The LicensePool's circulation data is what it was before.
        eq_(10, pool.licenses_owned)
        eq_(20, pool.licenses_available)
        eq_(30, pool.licenses_reserved)
        eq_(40, pool.patrons_in_hold_queue)

        # Work.update_time and LicensePool.last_checked are unaffected.
        eq_(None, work.last_update_time)
        eq_(None, pool.last_checked)

        # If we pass a mix of good and null values...
        pool.update_availability(5, None, None, None)

        # Only the good values are changed.
        eq_(5, pool.licenses_owned)
        eq_(20, pool.licenses_available)
        eq_(30, pool.licenses_reserved)
        eq_(40, pool.patrons_in_hold_queue)


    def test_open_access_links(self):
        edition, pool = self._edition(with_open_access_download=True)
        source = DataSource.lookup(self._db, DataSource.GUTENBERG)

        [oa1] = list(pool.open_access_links)

        # We have one open-access download, let's
        # add another.
        url = self._url
        media_type = Representation.EPUB_MEDIA_TYPE
        link2, new = pool.identifier.add_link(
            Hyperlink.OPEN_ACCESS_DOWNLOAD, url,
            source, media_type
        )
        oa2 = link2.resource

        # And let's add a link that's not an open-access download.
        url = self._url
        image, new = pool.identifier.add_link(
            Hyperlink.IMAGE, url, source, Representation.JPEG_MEDIA_TYPE
        )
        self._db.commit()

        # Only the two open-access download links show up.
        eq_(set([oa1, oa2]), set(pool.open_access_links))

    def test_better_open_access_pool_than(self):

        gutenberg_1 = self._licensepool(
            None, open_access=True, data_source_name=DataSource.GUTENBERG,
            with_open_access_download=True,
        )

        gutenberg_2 = self._licensepool(
            None, open_access=True, data_source_name=DataSource.GUTENBERG,
            with_open_access_download=True,
        )

        assert int(gutenberg_1.identifier.identifier) < int(gutenberg_2.identifier.identifier)

        standard_ebooks = self._licensepool(
            None, open_access=True, data_source_name=DataSource.STANDARD_EBOOKS,
            with_open_access_download=True
        )

        # Make sure Feedbooks data source exists -- it's not created
        # by default.
        feedbooks_data_source = DataSource.lookup(
            self._db, DataSource.FEEDBOOKS, autocreate=True
        )
        feedbooks = self._licensepool(
            None, open_access=True, data_source_name=DataSource.FEEDBOOKS,
            with_open_access_download=True
        )

        overdrive = self._licensepool(
            None, open_access=False, data_source_name=DataSource.OVERDRIVE
        )

        suppressed = self._licensepool(
            None, open_access=True, data_source_name=DataSource.GUTENBERG
        )
        suppressed.suppressed = True

        def better(x,y):
            return x.better_open_access_pool_than(y)

        # We would rather have nothing at all than a suppressed
        # LicensePool.
        eq_(False, better(suppressed, None))

        # A non-open-access LicensePool is not considered at all.
        eq_(False, better(overdrive, None))

        # Something is better than nothing.
        eq_(True, better(gutenberg_1, None))

        # An open access book from a high-quality source beats one
        # from a low-quality source.
        eq_(True, better(standard_ebooks, gutenberg_1))
        eq_(True, better(feedbooks, gutenberg_1))
        eq_(False, better(gutenberg_1, standard_ebooks))

        # A high Gutenberg number beats a low Gutenberg number.
        eq_(True, better(gutenberg_2, gutenberg_1))
        eq_(False, better(gutenberg_1, gutenberg_2))

        # If a supposedly open-access LicensePool doesn't have an
        # open-access download resource, it will only be considered if
        # there is no other alternative.
        no_resource = self._licensepool(
            None, open_access=True,
            data_source_name=DataSource.STANDARD_EBOOKS,
            with_open_access_download=False,
        )
        no_resource.open_access = True
        eq_(True, better(no_resource, None))
        eq_(False, better(no_resource, gutenberg_1))

    def test_with_complaint(self):
        library = self._default_library
        type = iter(Complaint.VALID_TYPES)
        type1 = next(type)
        type2 = next(type)
        type3 = next(type)

        work1 = self._work(
            "fiction work with complaint",
            language="eng",
            fiction=True,
            with_open_access_download=True)
        lp1 = work1.license_pools[0]
        lp1_complaint1 = self._complaint(
            lp1,
            type1,
            "lp1 complaint1 source",
            "lp1 complaint1 detail")
        lp1_complaint2 = self._complaint(
            lp1,
            type1,
            "lp1 complaint2 source",
            "lp1 complaint2 detail")
        lp1_complaint3 = self._complaint(
            lp1,
            type2,
            "work1 complaint3 source",
            "work1 complaint3 detail")
        lp1_resolved_complaint = self._complaint(
            lp1,
            type3,
            "work3 resolved complaint source",
            "work3 resolved complaint detail",
            datetime.datetime.now())

        work2 = self._work(
            "nonfiction work with complaint",
            language="eng",
            fiction=False,
            with_open_access_download=True)
        lp2 = work2.license_pools[0]
        lp2_complaint1 = self._complaint(
            lp2,
            type2,
            "work2 complaint1 source",
            "work2 complaint1 detail")
        lp2_resolved_complaint = self._complaint(
            lp2,
            type2,
            "work2 resolved complaint source",
            "work2 resolved complaint detail",
            datetime.datetime.now())

        work3 = self._work(
            "fiction work without complaint",
            language="eng",
            fiction=True,
            with_open_access_download=True)
        lp3 = work3.license_pools[0]
        lp3_resolved_complaint = self._complaint(
            lp3,
            type3,
            "work3 resolved complaint source",
            "work3 resolved complaint detail",
            datetime.datetime.now())

        work4 = self._work(
            "nonfiction work without complaint",
            language="eng",
            fiction=False,
            with_open_access_download=True)

        # excludes resolved complaints by default
        results = LicensePool.with_complaint(library).all()

        eq_(2, len(results))
        eq_(lp1.id, results[0][0].id)
        eq_(3, results[0][1])
        eq_(lp2.id, results[1][0].id)
        eq_(1, results[1][1])

        # include resolved complaints this time
        more_results = LicensePool.with_complaint(library, resolved=None).all()

        eq_(3, len(more_results))
        eq_(lp1.id, more_results[0][0].id)
        eq_(4, more_results[0][1])
        eq_(lp2.id, more_results[1][0].id)
        eq_(2, more_results[1][1])
        eq_(lp3.id, more_results[2][0].id)
        eq_(1, more_results[2][1])

        # show only resolved complaints
        resolved_results = LicensePool.with_complaint(
            library, resolved=True).all()
        lp_ids = set([result[0].id for result in resolved_results])
        counts = set([result[1] for result in resolved_results])

        eq_(3, len(resolved_results))
        eq_(lp_ids, set([lp1.id, lp2.id, lp3.id]))
        eq_(counts, set([1]))

        # This library has none of the license pools that have complaints,
        # so passing it in to with_complaint() gives no results.
        library2 = self._library()
        eq_(0, LicensePool.with_complaint(library2).count())

        # If we add the default library's collection to this new library,
        # we start getting the same results.
        library2.collections.extend(library.collections)
        eq_(3, LicensePool.with_complaint(library2, resolved=None).count())

    def test_set_presentation_edition(self):
        """
        Make sure composite edition creation makes good choices when combining
        field data from provider, metadata wrangler, admin interface, etc. editions.
        """
        # create different types of editions, all with the same identifier
        edition_admin = self._edition(data_source_name=DataSource.LIBRARY_STAFF, with_license_pool=False)
        edition_mw = self._edition(data_source_name=DataSource.METADATA_WRANGLER, with_license_pool=False)
        edition_od, pool = self._edition(data_source_name=DataSource.OVERDRIVE, with_license_pool=True)

        edition_mw.primary_identifier = pool.identifier
        edition_admin.primary_identifier = pool.identifier

        # set overlapping fields on editions
        edition_od.title = u"OverdriveTitle1"

        edition_mw.title = u"MetadataWranglerTitle1"
        edition_mw.subtitle = u"MetadataWranglerSubTitle1"

        edition_admin.title = u"AdminInterfaceTitle1"

        pool.set_presentation_edition()

        edition_composite = pool.presentation_edition

        assert_not_equal(edition_mw, edition_od)
        assert_not_equal(edition_od, edition_admin)
        assert_not_equal(edition_admin, edition_composite)
        assert_not_equal(edition_od, edition_composite)

        # make sure admin pool data had precedence
        eq_(edition_composite.title, u"AdminInterfaceTitle1")
        eq_(edition_admin.contributors, edition_composite.contributors)

        # make sure data not present in the higher-precedence editions didn't overwrite the lower-precedented editions' fields
        eq_(edition_composite.subtitle, u"MetadataWranglerSubTitle1")
        [license_pool] = edition_composite.is_presentation_for
        eq_(license_pool, pool)

        # Change the admin interface's opinion about who the author
        # is.
        for c in edition_admin.contributions:
            self._db.delete(c)
        self._db.commit()
        [jane], ignore = Contributor.lookup(self._db, u"Doe, Jane")
        jane.family_name, jane.display_name = jane.default_names()
        edition_admin.add_contributor(jane, Contributor.AUTHOR_ROLE)
        pool.set_presentation_edition()

        # The old contributor has been removed from the composite
        # edition, and the new contributor added.
        eq_(set([jane]), edition_composite.contributors)

    def test_circulation_changelog(self):

        edition, pool = self._edition(with_license_pool=True)
        pool.licenses_owned = 10
        pool.licenses_available = 9
        pool.licenses_reserved = 8
        pool.patrons_in_hold_queue = 7

        msg, args = pool.circulation_changelog(1, 2, 3, 4)

        # Since all four circulation values changed, the message is as
        # long as it could possibly get.
        eq_(
            'CHANGED %s "%s" %s (%s/%s) %s: %s=>%s %s: %s=>%s %s: %s=>%s %s: %s=>%s',
            msg
        )
        eq_(
            args,
            (edition.medium, edition.title, edition.author,
             pool.identifier.type, pool.identifier.identifier,
             'OWN', 1, 10, 'AVAIL', 2, 9, 'RSRV', 3, 8, 'HOLD', 4, 7)
        )

        # If only one circulation value changes, the message is a lot shorter.
        msg, args = pool.circulation_changelog(10, 9, 8, 15)
        eq_(
            'CHANGED %s "%s" %s (%s/%s) %s: %s=>%s',
            msg
        )
        eq_(
            args,
            (edition.medium, edition.title, edition.author,
             pool.identifier.type, pool.identifier.identifier,
             'HOLD', 15, 7)
        )

        # This works even if, for whatever reason, the edition's
        # bibliographic data is missing.
        edition.title = None
        edition.author = None

        msg, args = pool.circulation_changelog(10, 9, 8, 15)
        eq_("[NO TITLE]", args[1])
        eq_("[NO AUTHOR]", args[2])

    def test_update_availability_from_delta(self):
        """A LicensePool may have its availability information updated based
        on a single observed change.
        """

        edition, pool = self._edition(with_license_pool=True)
        eq_(None, pool.last_checked)
        eq_(1, pool.licenses_owned)
        eq_(1, pool.licenses_available)

        add = CirculationEvent.DISTRIBUTOR_LICENSE_ADD
        checkout = CirculationEvent.DISTRIBUTOR_CHECKOUT
        analytics = MockAnalyticsProvider()
        eq_(0, analytics.count)

        # This observation has no timestamp, but the pool has no
        # history, so we process it.
        pool.update_availability_from_delta(add, CirculationEvent.NO_DATE, 1, analytics)
        eq_(None, pool.last_checked)
        eq_(2, pool.licenses_owned)
        eq_(2, pool.licenses_available)

        # Processing triggered two analytics events -- one for creating
        # the license pool and one for making it available.
        eq_(2, analytics.count)

        # Now the pool has a history, and we can't fit an undated
        # observation into that history, so undated observations
        # have no effect on circulation data.
        now = datetime.datetime.utcnow()
        yesterday = now - datetime.timedelta(days=1)
        pool.last_checked = yesterday
        pool.update_availability_from_delta(add, CirculationEvent.NO_DATE, 1, analytics)
        eq_(2, pool.licenses_owned)
        eq_(yesterday, pool.last_checked)

        # However, outdated events are passed on to analytics so that
        # we record the fact that they happened... at some point.
        eq_(3, analytics.count)

        # This observation is more recent than the last time the pool
        # was checked, so it's processed and the last check time is
        # updated.
        pool.update_availability_from_delta(checkout, now, 1, analytics)
        eq_(2, pool.licenses_owned)
        eq_(1, pool.licenses_available)
        eq_(now, pool.last_checked)
        eq_(4, analytics.count)

        # This event is less recent than the last time the pool was
        # checked, so it's ignored. Processing it is likely to do more
        # harm than good.
        pool.update_availability_from_delta(add, yesterday, 1, analytics)
        eq_(2, pool.licenses_owned)
        eq_(now, pool.last_checked)

        # It's still logged to analytics, though.
        eq_(5, analytics.count)

        # This event is new but does not actually cause the
        # circulation to change at all.
        pool.update_availability_from_delta(add, now, 0, analytics)
        eq_(2, pool.licenses_owned)
        eq_(now, pool.last_checked)

        # We still send the analytics event.
        eq_(6, analytics.count)

    def test_calculate_change_from_one_event(self):
        """Test the internal method called by update_availability_from_delta."""
        CE = CirculationEvent

        # Create a LicensePool with a large number of available licenses.
        edition, pool = self._edition(with_license_pool=True)
        pool.licenses_owned = 5
        pool.licenses_available = 4
        pool.licenses_reserved = 0
        pool.patrons_in_hold_queue = 0

        # Calibrate _calculate_change_from_one_event by sending it an
        # event that makes no difference. This lets us see what a
        # 'status quo' response from the method would look like.
        calc = pool._calculate_change_from_one_event
        eq_((5,4,0,0), calc(CE.DISTRIBUTOR_CHECKIN, 0))

        # If there ever appear to be more licenses available than
        # owned, the number of owned licenses is left alone. It's
        # possible that we have more licenses than we thought, but
        # it's more likely that a license has expired or otherwise
        # been removed.
        eq_((5,5,0,0), calc(CE.DISTRIBUTOR_CHECKIN, 3))

        # But we don't bump up the number of available licenses just
        # because one becomes available.
        eq_((5,5,0,0), calc(CE.DISTRIBUTOR_CHECKIN, 1))

        # When you signal a hold on a book that's available, we assume
        # that the book has stopped being available.
        eq_((5,0,0,3), calc(CE.DISTRIBUTOR_HOLD_PLACE, 3))

        # If a license stops being owned, it implicitly stops being
        # available. (But we don't know if the license that became
        # unavailable is one of the ones currently checked out to
        # someone, or one of the other ones.)
        eq_((3,3,0,0), calc(CE.DISTRIBUTOR_LICENSE_REMOVE, 2))

        # If a license stops being available, it doesn't stop
        # being owned.
        eq_((5,3,0,0), calc(CE.DISTRIBUTOR_CHECKOUT, 1))

        # None of these numbers will go below zero.
        eq_((0,0,0,0), calc(CE.DISTRIBUTOR_LICENSE_REMOVE, 100))

        # Newly added licenses start out available if there are no
        # patrons in the hold queue.
        eq_((6,5,0,0), calc(CE.DISTRIBUTOR_LICENSE_ADD, 1))

        # Now let's run some tests with a LicensePool that has a large holds
        # queue.
        pool.licenses_owned = 5
        pool.licenses_available = 0
        pool.licenses_reserved = 1
        pool.patrons_in_hold_queue = 3
        eq_((5,0,1,3), calc(CE.DISTRIBUTOR_HOLD_PLACE, 0))

        # When you signal a hold on a book that already has holds, it
        # does nothing but increase the number of patrons in the hold
        # queue.
        eq_((5,0,1,6), calc(CE.DISTRIBUTOR_HOLD_PLACE, 3))

        # A checkin event has no effect...
        eq_((5,0,1,3), calc(CE.DISTRIBUTOR_CHECKIN, 1))

        # ...because it's presumed that it will be followed by an
        # availability notification event, which takes a patron off
        # the hold queue and adds them to the reserved list.
        eq_((5,0,2,2), calc(CE.DISTRIBUTOR_AVAILABILITY_NOTIFY, 1))

        # The only exception is if the checkin event wipes out the
        # entire holds queue, in which case the number of available
        # licenses increases.  (But nothing else changes -- we're
        # still waiting for the availability notification events.)
        eq_((5,3,1,3), calc(CE.DISTRIBUTOR_CHECKIN, 6))

        # Again, note that even though six copies were checked in,
        # we're not assuming we own more licenses than we
        # thought. It's more likely that the sixth license expired and
        # we weren't notified.

        # When there are no licenses available, a checkout event
        # draws from the pool of licenses reserved instead.
        eq_((5,0,0,3), calc(CE.DISTRIBUTOR_CHECKOUT, 2))

        # Newly added licenses do not start out available if there are
        # patrons in the hold queue.
        eq_((6,0,1,3), calc(CE.DISTRIBUTOR_LICENSE_ADD, 1))


class TestLicensePoolDeliveryMechanism(DatabaseTest):

    def test_lpdm_change_may_change_open_access_status(self):
        # Here's a book that's not open access.
        edition, pool = self._edition(with_license_pool=True)
        eq_(False, pool.open_access)

        # We're going to use LicensePoolDeliveryMechanism.set to
        # to give it a non-open-access LPDM.
        data_source = pool.data_source
        identifier = pool.identifier
        content_type = Representation.EPUB_MEDIA_TYPE
        drm_scheme = DeliveryMechanism.NO_DRM
        LicensePoolDeliveryMechanism.set(
            data_source, identifier, content_type, drm_scheme,
            RightsStatus.IN_COPYRIGHT
        )

        # Now there's a way to get the book, but it's not open access.
        eq_(False, pool.open_access)

        # Now give it an open-access LPDM.
        link, new = pool.identifier.add_link(
            Hyperlink.OPEN_ACCESS_DOWNLOAD, self._url,
            data_source, content_type
        )
        oa_lpdm = LicensePoolDeliveryMechanism.set(
            data_source, identifier, content_type, drm_scheme,
            RightsStatus.GENERIC_OPEN_ACCESS, link.resource
        )

        # Now it's open access.
        eq_(True, pool.open_access)

        # Delete the open-access LPDM, and it stops being open access.
        oa_lpdm.delete()
        eq_(False, pool.open_access)

    def test_set_rights_status(self):
        # Here's a non-open-access book.
        edition, pool = self._edition(with_license_pool=True)
        pool.open_access = False
        [lpdm] = pool.delivery_mechanisms

        # We set its rights status to 'in copyright', and nothing changes.
        uri = RightsStatus.IN_COPYRIGHT
        status = lpdm.set_rights_status(uri)
        eq_(status, lpdm.rights_status)
        eq_(uri, status.uri)
        eq_(RightsStatus.NAMES.get(uri), status.name)
        eq_(False, pool.open_access)

        # Setting it again won't change anything.
        status2 = lpdm.set_rights_status(uri)
        eq_(status, status2)

        # Set the rights status to a different URL, we change to a different
        # RightsStatus object.
        uri2 = "http://unknown"
        status3 = lpdm.set_rights_status(uri2)
        assert status != status3
        eq_(RightsStatus.UNKNOWN, status3.uri)
        eq_(RightsStatus.NAMES.get(RightsStatus.UNKNOWN), status3.name)

        # Set the rights status to a URL that implies open access,
        # and the status of the LicensePool is changed.
        open_access_uri = RightsStatus.GENERIC_OPEN_ACCESS
        open_access_status = lpdm.set_rights_status(open_access_uri)
        eq_(open_access_uri, open_access_status.uri)
        eq_(RightsStatus.NAMES.get(open_access_uri), open_access_status.name)
        eq_(True, pool.open_access)

        # Set it back to a URL that does not imply open access, and
        # the status of the LicensePool is changed back.
        non_open_access_status = lpdm.set_rights_status(uri)
        eq_(False, pool.open_access)

        # Now add a second delivery mechanism, so the pool has one
        # open-access and one commercial delivery mechanism.
        lpdm2 = pool.set_delivery_mechanism(
            Representation.EPUB_MEDIA_TYPE, DeliveryMechanism.NO_DRM,
            RightsStatus.CC_BY, None)
        eq_(2, len(pool.delivery_mechanisms))

        # Now the pool is open access again
        eq_(True, pool.open_access)

        # But if we change the new delivery mechanism to non-open
        # access, the pool won't be open access anymore either.
        lpdm2.set_rights_status(uri)
        eq_(False, pool.open_access)

    def test_uniqueness_constraint(self):
        # with_open_access_download will create a LPDM
        # for the open-access download.
        edition, pool = self._edition(with_license_pool=True,
                                      with_open_access_download=True)
        [lpdm] = pool.delivery_mechanisms

        # We can create a second LPDM with the same data type and DRM status,
        # so long as the resource is different.
        link, new = pool.identifier.add_link(
            Hyperlink.OPEN_ACCESS_DOWNLOAD, self._url,
            pool.data_source, "text/html"
        )
        lpdm2 = pool.set_delivery_mechanism(
            lpdm.delivery_mechanism.content_type,
            lpdm.delivery_mechanism.drm_scheme,
            lpdm.rights_status.uri,
            link.resource,
        )
        eq_(lpdm2.delivery_mechanism, lpdm.delivery_mechanism)
        assert lpdm2.resource != lpdm.resource

    def test_compatible_with(self):
        """Test the rules about which LicensePoolDeliveryMechanisms are
        mutually compatible and which are mutually exclusive.
        """

        edition, pool = self._edition(with_license_pool=True,
                                      with_open_access_download=True)
        [mech] = pool.delivery_mechanisms

        # Test the simple cases.
        eq_(False, mech.compatible_with(None))
        eq_(False, mech.compatible_with("Not a LicensePoolDeliveryMechanism"))
        eq_(True, mech.compatible_with(mech))

        # Now let's set up a scenario that works and then see how it fails.
        self._add_generic_delivery_mechanism(pool)

        # This book has two different LicensePoolDeliveryMechanisms
        # with the same underlying DeliveryMechanism. They're
        # compatible.
        [mech1, mech2] = pool.delivery_mechanisms
        assert mech1.id != mech2.id
        eq_(mech1.delivery_mechanism, mech2.delivery_mechanism)
        eq_(True, mech1.compatible_with(mech2))

        # The LicensePoolDeliveryMechanisms must identify the same
        # book from the same data source.
        mech1.data_source_id = self._id
        eq_(False, mech1.compatible_with(mech2))

        mech1.data_source_id = mech2.data_source_id
        mech1.identifier_id = self._id
        eq_(False, mech1.compatible_with(mech2))
        mech1.identifier_id = mech2.identifier_id

        # The underlying delivery mechanisms don't have to be exactly
        # the same, but they must be compatible.
        pdf_adobe, ignore = DeliveryMechanism.lookup(
            self._db, Representation.PDF_MEDIA_TYPE,
            DeliveryMechanism.ADOBE_DRM
        )
        mech1.delivery_mechanism = pdf_adobe
        self._db.commit()
        eq_(False, mech1.compatible_with(mech2))

        streaming, ignore = DeliveryMechanism.lookup(
            self._db, DeliveryMechanism.STREAMING_TEXT_CONTENT_TYPE,
            DeliveryMechanism.STREAMING_DRM
        )
        mech1.delivery_mechanism = streaming
        self._db.commit()
        eq_(True, mech1.compatible_with(mech2))

    def test_compatible_with_calls_compatible_with_on_deliverymechanism(self):
        # Create two LicensePoolDeliveryMechanisms with different
        # media types.
        edition, pool = self._edition(with_license_pool=True,
                                      with_open_access_download=True)
        self._add_generic_delivery_mechanism(pool)
        [mech1, mech2] = pool.delivery_mechanisms
        mech2.delivery_mechanism, ignore = DeliveryMechanism.lookup(
            self._db, Representation.PDF_MEDIA_TYPE, DeliveryMechanism.NO_DRM
        )
        self._db.commit()

        eq_(True, mech1.is_open_access)
        eq_(False, mech2.is_open_access)

        # Determining whether the mechanisms are compatible requires
        # calling compatible_with on the first mechanism's
        # DeliveryMechanism, passing in the second DeliveryMechanism
        # plus the answer to 'are both LicensePoolDeliveryMechanisms
        # open-access?'
        class Mock(object):
            called_with = None
            @classmethod
            def compatible_with(cls, other, open_access):
                cls.called_with = (other, open_access)
                return True
        mech1.delivery_mechanism.compatible_with = Mock.compatible_with

        # Call compatible_with, and the mock method is called with the
        # second DeliveryMechanism and (since one of the
        # LicensePoolDeliveryMechanisms is not open-access) the value
        # False.
        mech1.compatible_with(mech2)
        eq_(
            (mech2.delivery_mechanism, False),
            Mock.called_with
        )

        # If both LicensePoolDeliveryMechanisms are open-access,
        # True is passed in instead, so that
        # DeliveryMechanism.compatible_with() applies the less strict
        # compatibility rules for open-access fulfillment.
        mech2.set_rights_status(RightsStatus.GENERIC_OPEN_ACCESS)
        mech1.compatible_with(mech2)
        eq_(
            (mech2.delivery_mechanism, True),
            Mock.called_with
        )
class TestDeliveryMechanism(DatabaseTest):

    def setup(self):
        super(TestDeliveryMechanism, self).setup()
        self.epub_no_drm, ignore = DeliveryMechanism.lookup(
            self._db, Representation.EPUB_MEDIA_TYPE, DeliveryMechanism.NO_DRM)
        self.epub_adobe_drm, ignore = DeliveryMechanism.lookup(
            self._db, Representation.EPUB_MEDIA_TYPE, DeliveryMechanism.ADOBE_DRM)
        self.overdrive_streaming_text, ignore = DeliveryMechanism.lookup(
            self._db, DeliveryMechanism.STREAMING_TEXT_CONTENT_TYPE, DeliveryMechanism.OVERDRIVE_DRM)

    def test_implicit_medium(self):
        eq_(Edition.BOOK_MEDIUM, self.epub_no_drm.implicit_medium)
        eq_(Edition.BOOK_MEDIUM, self.epub_adobe_drm.implicit_medium)
        eq_(Edition.BOOK_MEDIUM, self.overdrive_streaming_text.implicit_medium)

    def test_is_media_type(self):
        eq_(False, DeliveryMechanism.is_media_type(None))
        eq_(True, DeliveryMechanism.is_media_type(Representation.EPUB_MEDIA_TYPE))
        eq_(False, DeliveryMechanism.is_media_type(DeliveryMechanism.KINDLE_CONTENT_TYPE))
        eq_(False, DeliveryMechanism.is_media_type(DeliveryMechanism.STREAMING_TEXT_CONTENT_TYPE))

    def test_is_streaming(self):
        eq_(False, self.epub_no_drm.is_streaming)
        eq_(False, self.epub_adobe_drm.is_streaming)
        eq_(True, self.overdrive_streaming_text.is_streaming)

    def test_drm_scheme_media_type(self):
        eq_(None, self.epub_no_drm.drm_scheme_media_type)
        eq_(DeliveryMechanism.ADOBE_DRM, self.epub_adobe_drm.drm_scheme_media_type)
        eq_(None, self.overdrive_streaming_text.drm_scheme_media_type)

    def test_content_type_media_type(self):
        eq_(Representation.EPUB_MEDIA_TYPE, self.epub_no_drm.content_type_media_type)
        eq_(Representation.EPUB_MEDIA_TYPE, self.epub_adobe_drm.content_type_media_type)
        eq_(Representation.TEXT_HTML_MEDIA_TYPE + DeliveryMechanism.STREAMING_PROFILE,
            self.overdrive_streaming_text.content_type_media_type)

    def test_default_fulfillable(self):
        mechanism, is_new = DeliveryMechanism.lookup(
            self._db, Representation.EPUB_MEDIA_TYPE,
            DeliveryMechanism.ADOBE_DRM
        )
        eq_(False, is_new)
        eq_(True, mechanism.default_client_can_fulfill)

        mechanism, is_new = DeliveryMechanism.lookup(
            self._db, Representation.PDF_MEDIA_TYPE,
            DeliveryMechanism.STREAMING_DRM
        )
        eq_(True, is_new)
        eq_(False, mechanism.default_client_can_fulfill)

    def test_association_with_licensepool(self):
        ignore, with_download = self._edition(with_open_access_download=True)
        [lpmech] = with_download.delivery_mechanisms
        eq_("Dummy content", lpmech.resource.representation.content)
        mech = lpmech.delivery_mechanism
        eq_(Representation.EPUB_MEDIA_TYPE, mech.content_type)
        eq_(mech.NO_DRM, mech.drm_scheme)

    def test_compatible_with(self):
        """Test the rules about which DeliveryMechanisms are
        mutually compatible and which are mutually exclusive.
        """
        epub_adobe, ignore = DeliveryMechanism.lookup(
            self._db, Representation.EPUB_MEDIA_TYPE,
            DeliveryMechanism.ADOBE_DRM
        )

        pdf_adobe, ignore = DeliveryMechanism.lookup(
            self._db, Representation.PDF_MEDIA_TYPE,
            DeliveryMechanism.ADOBE_DRM
        )

        epub_no_drm, ignore = DeliveryMechanism.lookup(
            self._db, Representation.EPUB_MEDIA_TYPE,
            DeliveryMechanism.NO_DRM
        )

        pdf_no_drm, ignore = DeliveryMechanism.lookup(
            self._db, Representation.PDF_MEDIA_TYPE,
            DeliveryMechanism.NO_DRM
        )

        streaming, ignore = DeliveryMechanism.lookup(
            self._db, DeliveryMechanism.STREAMING_TEXT_CONTENT_TYPE,
            DeliveryMechanism.STREAMING_DRM
        )

        # A non-streaming DeliveryMechanism is compatible only with
        # itself or a streaming mechanism.
        eq_(False, epub_adobe.compatible_with(None))
        eq_(False, epub_adobe.compatible_with("Not a DeliveryMechanism"))
        eq_(False, epub_adobe.compatible_with(epub_no_drm))
        eq_(False, epub_adobe.compatible_with(pdf_adobe))
        eq_(False, epub_no_drm.compatible_with(pdf_no_drm))
        eq_(True, epub_adobe.compatible_with(epub_adobe))
        eq_(True, epub_adobe.compatible_with(streaming))

        # A streaming mechanism is compatible with anything.
        eq_(True, streaming.compatible_with(epub_adobe))
        eq_(True, streaming.compatible_with(pdf_adobe))
        eq_(True, streaming.compatible_with(epub_no_drm))

        # Rules are slightly different for open-access books: books
        # in any format are compatible so long as they have no DRM.
        eq_(True, epub_no_drm.compatible_with(pdf_no_drm, True))
        eq_(False, epub_no_drm.compatible_with(pdf_adobe, True))


class TestRightsStatus(DatabaseTest):

    def test_lookup(self):
        status = RightsStatus.lookup(self._db, RightsStatus.IN_COPYRIGHT)
        eq_(RightsStatus.IN_COPYRIGHT, status.uri)
        eq_(RightsStatus.NAMES.get(RightsStatus.IN_COPYRIGHT), status.name)

        status = RightsStatus.lookup(self._db, RightsStatus.CC0)
        eq_(RightsStatus.CC0, status.uri)
        eq_(RightsStatus.NAMES.get(RightsStatus.CC0), status.name)

        status = RightsStatus.lookup(self._db, "not a known rights uri")
        eq_(RightsStatus.UNKNOWN, status.uri)
        eq_(RightsStatus.NAMES.get(RightsStatus.UNKNOWN), status.name)

    def test_unique_uri_constraint(self):
        # We already have this RightsStatus.
        status = RightsStatus.lookup(self._db, RightsStatus.IN_COPYRIGHT)

        # Let's try to create another one with the same URI.
        dupe = RightsStatus(uri=RightsStatus.IN_COPYRIGHT)
        self._db.add(dupe)

        # Nope.
        assert_raises(IntegrityError, self._db.commit)

class TestComplaint(DatabaseTest):

    def setup(self):
        super(TestComplaint, self).setup()
        self.edition, self.pool = self._edition(with_license_pool=True)
        self.type = "http://librarysimplified.org/terms/problem/wrong-genre"

    def test_for_license_pool(self):
        work_complaint, is_new = Complaint.register(
            self.pool, self.type, "yes", "okay"
        )

        lp_type = self.type.replace('wrong-genre', 'cannot-render')
        lp_complaint, is_new = Complaint.register(
            self.pool, lp_type, "yes", "okay")

        eq_(False, work_complaint.for_license_pool)
        eq_(True, lp_complaint.for_license_pool)

    def test_success(self):
        complaint, is_new = Complaint.register(
            self.pool, self.type, "foo", "bar"
        )
        eq_(True, is_new)
        eq_(self.type, complaint.type)
        eq_("foo", complaint.source)
        eq_("bar", complaint.detail)
        assert abs(datetime.datetime.utcnow() -complaint.timestamp).seconds < 3

        # A second complaint from the same source is folded into the
        # original complaint.
        complaint2, is_new = Complaint.register(
            self.pool, self.type, "foo", "baz"
        )
        eq_(False, is_new)
        eq_(complaint.id, complaint2.id)
        eq_("baz", complaint.detail)

        eq_(1, len(self.pool.complaints))

    def test_success_no_source(self):
        complaint, is_new = Complaint.register(
            self.pool, self.type, None, None
        )
        eq_(True, is_new)
        eq_(self.type, complaint.type)
        eq_(None, complaint.source)

        # A second identical complaint from no source is treated as a
        # separate complaint.
        complaint2, is_new = Complaint.register(
            self.pool, self.type, None, None
        )
        eq_(True, is_new)
        eq_(None, complaint.source)
        assert complaint2.id != complaint.id

        eq_(2, len(self.pool.complaints))

    def test_failure_no_licensepool(self):
        assert_raises(
            ValueError, Complaint.register, self.pool, type, None, None
        )

    def test_unrecognized_type(self):
        type = "http://librarysimplified.org/terms/problem/no-such-error"
        assert_raises(
            ValueError, Complaint.register, self.pool, type, None, None
        )

    def test_register_resolved(self):
        complaint, is_new = Complaint.register(
            self.pool, self.type, "foo", "bar", resolved=datetime.datetime.utcnow()
        )
        eq_(True, is_new)
        eq_(self.type, complaint.type)
        eq_("foo", complaint.source)
        eq_("bar", complaint.detail)
        assert abs(datetime.datetime.utcnow() -complaint.timestamp).seconds < 3
        assert abs(datetime.datetime.utcnow() -complaint.resolved).seconds < 3

        # A second complaint from the same source is not folded into the same complaint.
        complaint2, is_new = Complaint.register(
            self.pool, self.type, "foo", "baz"
        )
        eq_(True, is_new)
        assert complaint2.id != complaint.id
        eq_("baz", complaint2.detail)
        eq_(2, len(self.pool.complaints))

    def test_resolve(self):
        complaint, is_new = Complaint.register(
            self.pool, self.type, "foo", "bar"
        )
        complaint.resolve()
        assert complaint.resolved != None
        assert abs(datetime.datetime.utcnow() - complaint.resolved).seconds < 3

class TestIntegrationClient(DatabaseTest):

    def setup(self):
        super(TestIntegrationClient, self).setup()
        self.client = self._integration_client()

    def test_for_url(self):
        now = datetime.datetime.utcnow()
        url = self._url
        client, is_new = IntegrationClient.for_url(self._db, url)

        # A new IntegrationClient has been created.
        eq_(True, is_new)

        # Its .url is a normalized version of the provided URL.
        eq_(client.url, IntegrationClient.normalize_url(url))

        # It has timestamps for created & last_accessed.
        assert client.created and client.last_accessed
        assert client.created > now
        eq_(True, isinstance(client.created, datetime.datetime))
        eq_(client.created, client.last_accessed)

        # It does not have a shared secret.
        eq_(None, client.shared_secret)

        # Calling it again on the same URL gives the same object.
        client2, is_new = IntegrationClient.for_url(self._db, url)
        eq_(client, client2)

    def test_register(self):
        now = datetime.datetime.utcnow()
        client, is_new = IntegrationClient.register(self._db, self._url)

        # It creates a shared_secret.
        assert client.shared_secret
        # And sets a timestamp for created & last_accessed.
        assert client.created and client.last_accessed
        assert client.created > now
        eq_(True, isinstance(client.created, datetime.datetime))
        eq_(client.created, client.last_accessed)

        # It raises an error if the url is already registered and the
        # submitted shared_secret is inaccurate.
        assert_raises(ValueError, IntegrationClient.register, self._db, client.url)
        assert_raises(ValueError, IntegrationClient.register, self._db, client.url, 'wrong')

    def test_authenticate(self):

        result = IntegrationClient.authenticate(self._db, u"secret")
        eq_(self.client, result)

        result = IntegrationClient.authenticate(self._db, u"wrong_secret")
        eq_(None, result)

    def test_normalize_url(self):
        # http/https protocol is removed.
        url = 'https://fake.com'
        eq_('fake.com', IntegrationClient.normalize_url(url))

        url = 'http://really-fake.com'
        eq_('really-fake.com', IntegrationClient.normalize_url(url))

        # www is removed if it exists, along with any trailing /
        url = 'https://www.also-fake.net/'
        eq_('also-fake.net', IntegrationClient.normalize_url(url))

        # Subdomains and paths are retained.
        url = 'https://www.super.fake.org/wow/'
        eq_('super.fake.org/wow', IntegrationClient.normalize_url(url))

        # URL is lowercased.
        url = 'http://OMG.soVeryFake.gov'
        eq_('omg.soveryfake.gov', IntegrationClient.normalize_url(url))
