# encoding: utf-8

from nose.tools import (
    assert_raises,
    assert_raises_regexp,
    assert_not_equal,
    eq_,
    set_trace,
)
import core.model
import core.model.bibliographic_metadata
from core.model.bibliographic_metadata import *

import datetime
import feedparser
from lxml import etree

from sqlalchemy.orm.exc import NoResultFound
from .. import DatabaseTest

class TestDataSource(DatabaseTest):

    def test_lookup(self):
        key = DataSource.GUTENBERG

        # Unlike with most of these tests, this cache doesn't start
        # out empty. It's populated with all known values at the start
        # of the test. Let's reset the cache.
        DataSource.reset_cache()

        gutenberg = DataSource.lookup(self._db, DataSource.GUTENBERG)
        eq_(key, gutenberg.name)
        eq_(True, gutenberg.offers_licenses)
        eq_(key, gutenberg.cache_key())

        # Object has been loaded into cache.
        eq_((gutenberg, False), DataSource.by_cache_key(self._db, key, None))

        # Now try creating a new data source.
        key = "New data source"

        # It's not in the cache.
        eq_((None, False), DataSource.by_cache_key(self._db, key, None))

        new_source = DataSource.lookup(
            self._db, key, autocreate=True, offers_licenses=True
        )

        # A new data source has been created.
        eq_(key, new_source.name)
        eq_(True, new_source.offers_licenses)

        # The cache was reset when the data source was created.
        eq_(HasFullTableCache.RESET, DataSource._cache)

        eq_((new_source, False), DataSource.by_cache_key(self._db, key, None))

    def test_lookup_by_deprecated_name(self):
        threem = DataSource.lookup(self._db, "3M")
        eq_(DataSource.BIBLIOTHECA, threem.name)
        assert DataSource.BIBLIOTHECA != "3M"

    def test_lookup_returns_none_for_nonexistent_source(self):
        eq_(None, DataSource.lookup(
            self._db, "No such data source " + self._str))

    def test_lookup_with_autocreate(self):
        name = "Brand new data source " + self._str
        new_source = DataSource.lookup(self._db, name, autocreate=True)
        eq_(name, new_source.name)
        eq_(False, new_source.offers_licenses)

        name = "New data source with licenses" + self._str
        new_source = DataSource.lookup(
            self._db, name, autocreate=True, offers_licenses=True
        )
        eq_(True, new_source.offers_licenses)

    def test_metadata_sources_for(self):
        content_cafe = DataSource.lookup(self._db, DataSource.CONTENT_CAFE)
        isbn_metadata_sources = DataSource.metadata_sources_for(
            self._db, Identifier.ISBN
        )

        eq_(1, len(isbn_metadata_sources))
        eq_([content_cafe], isbn_metadata_sources)

    def test_license_source_for(self):
        identifier = self._identifier(Identifier.OVERDRIVE_ID)
        source = DataSource.license_source_for(self._db, identifier)
        eq_(DataSource.OVERDRIVE, source.name)

    def test_license_source_for_string(self):
        source = DataSource.license_source_for(
            self._db, Identifier.THREEM_ID)
        eq_(DataSource.THREEM, source.name)

    def test_license_source_fails_if_identifier_type_does_not_provide_licenses(self):
        identifier = self._identifier(DataSource.MANUAL)
        assert_raises(
            NoResultFound, DataSource.license_source_for, self._db, identifier)

class TestIdentifier(DatabaseTest):

    def test_for_foreign_id(self):
        identifier_type = Identifier.ISBN
        isbn = "3293000061"

        # Getting the data automatically creates a database record.
        identifier, was_new = Identifier.for_foreign_id(
            self._db, identifier_type, isbn)
        eq_(Identifier.ISBN, identifier.type)
        eq_(isbn, identifier.identifier)
        eq_(True, was_new)

        # If we get it again we get the same data, but it's no longer new.
        identifier2, was_new = Identifier.for_foreign_id(
            self._db, identifier_type, isbn)
        eq_(identifier, identifier2)
        eq_(False, was_new)

        # If we pass in no data we get nothing back.
        eq_(None, Identifier.for_foreign_id(self._db, None, None))

    def test_for_foreign_id_by_deprecated_type(self):
        threem_id, is_new = Identifier.for_foreign_id(
            self._db, "3M ID", self._str
        )
        eq_(Identifier.BIBLIOTHECA_ID, threem_id.type)
        assert Identifier.BIBLIOTHECA_ID != "3M ID"

    def test_for_foreign_id_rejects_invalid_identifiers(self):
        assert_raises_regexp(
            ValueError,
            '"foo/bar" is not a valid Bibliotheca ID.',
            Identifier.for_foreign_id,
            self._db, Identifier.BIBLIOTHECA_ID, "foo/bar"
        )

    def test_valid_as_foreign_identifier(self):
        m = Identifier.valid_as_foreign_identifier

        eq_(True, m(Identifier.BIBLIOTHECA_ID, "bhhot389"))
        eq_(False, m(Identifier.BIBLIOTHECA_ID, "bhhot389/open_book"))
        eq_(False, m(Identifier.BIBLIOTHECA_ID, "bhhot389,bhhot389"))

        eq_(True, m(Identifier.BIBLIOTHECA_ID, "0015142259"))
        eq_(False, m(Identifier.BIBLIOTHECA_ID, "0015142259,0015187940"))

    def test_for_foreign_id_without_autocreate(self):
        identifier_type = Identifier.ISBN
        isbn = self._str

        # We don't want to auto-create a database record, so we set
        # autocreate=False
        identifier, was_new = Identifier.for_foreign_id(
            self._db, identifier_type, isbn, autocreate=False)
        eq_(None, identifier)
        eq_(False, was_new)

    def test_from_asin(self):
        isbn10 = '1449358063'
        isbn13 = '9781449358068'
        asin = 'B0088IYM3C'
        isbn13_with_dashes = '978-144-935-8068'

        i_isbn10, new1 = Identifier.from_asin(self._db, isbn10)
        i_isbn13, new2 = Identifier.from_asin(self._db, isbn13)
        i_asin, new3 = Identifier.from_asin(self._db, asin)
        i_isbn13_2, new4 = Identifier.from_asin(self._db, isbn13_with_dashes)

        # The three ISBNs are equivalent, so they got turned into the same
        # Identifier, using the ISBN13.
        eq_(i_isbn10, i_isbn13)
        eq_(i_isbn13_2, i_isbn13)
        eq_(Identifier.ISBN, i_isbn10.type)
        eq_(isbn13, i_isbn10.identifier)
        eq_(True, new1)
        eq_(False, new2)
        eq_(False, new4)

        eq_(Identifier.ASIN, i_asin.type)
        eq_(asin, i_asin.identifier)

    def test_urn(self):
        # ISBN identifiers use the ISBN URN scheme.
        identifier, ignore = Identifier.for_foreign_id(
            self._db, Identifier.ISBN, "9781449358068")
        eq_("urn:isbn:9781449358068", identifier.urn)

        # URI identifiers don't need a URN scheme.
        identifier, ignore = Identifier.for_foreign_id(
            self._db, Identifier.URI, "http://example.com/")
        eq_(identifier.identifier, identifier.urn)

        # Gutenberg identifiers use Gutenberg's URL-based sceheme
        identifier = self._identifier(Identifier.GUTENBERG_ID)
        eq_(Identifier.GUTENBERG_URN_SCHEME_PREFIX + identifier.identifier,
            identifier.urn)

        # All other identifiers use our custom URN scheme.
        identifier = self._identifier(Identifier.OVERDRIVE_ID)
        assert identifier.urn.startswith(Identifier.URN_SCHEME_PREFIX)

    def test_parse_urns(self):
        identifier = self._identifier()
        fake_urn = "what_even_is_this"
        new_urn = Identifier.URN_SCHEME_PREFIX + "Overdrive%20ID/nosuchidentifier"
        # Also create a different URN that would result in the same identifier.
        same_new_urn = Identifier.URN_SCHEME_PREFIX + "Overdrive%20ID/NOSUCHidentifier"
        urns = [identifier.urn, fake_urn, new_urn, same_new_urn]

        results = Identifier.parse_urns(self._db, urns, autocreate=False)
        identifiers_by_urn, failures = results

        # By default, no new identifiers are created. All URNs for identifiers
        # that aren't in the db are included in the list of failures.
        eq_(sorted([fake_urn, new_urn, same_new_urn]), sorted(failures))

        # Only the existing identifier is included in the results.
        eq_(1, len(identifiers_by_urn))
        eq_({identifier.urn : identifier}, identifiers_by_urn)

        # By default, new identifiers are created, too.
        results = Identifier.parse_urns(self._db, urns)
        identifiers_by_urn, failures = results

        # Only the fake URN is returned as a failure.
        eq_([fake_urn], failures)

        # Only two additional identifiers have been created.
        eq_(2, len(identifiers_by_urn))

        # One is the existing identifier.
        eq_(identifier, identifiers_by_urn[identifier.urn])

        # And the new identifier has been created.
        new_identifier = identifiers_by_urn[new_urn]
        assert isinstance(new_identifier, Identifier)
        assert new_identifier in self._db
        eq_(Identifier.OVERDRIVE_ID, new_identifier.type)
        eq_("nosuchidentifier", new_identifier.identifier)

        # By passing in a list of allowed_types we can stop certain
        # types of Identifiers from being looked up, even if they
        # already exist.
        isbn_urn = "urn:isbn:9781453219539"
        urns = [new_urn, isbn_urn]
        only_overdrive = [Identifier.OVERDRIVE_ID]
        only_isbn = [Identifier.OVERDRIVE_ID]
        everything = []

        success, failure = Identifier.parse_urns(
            self._db, urns, allowed_types=[Identifier.OVERDRIVE_ID]
        )
        assert new_urn in success
        assert isbn_urn in failure

        success, failure = Identifier.parse_urns(
            self._db, urns, allowed_types=[
                Identifier.OVERDRIVE_ID, Identifier.ISBN
            ]
        )
        assert new_urn in success
        assert isbn_urn in success
        eq_([], failure)

        # If the allowed_types is empty, no URNs can be looked up
        # -- this is most likely the caller's mistake.
        success, failure = Identifier.parse_urns(
            self._db, urns, allowed_types=[]
        )
        assert new_urn in failure
        assert isbn_urn in failure

    def test_parse_urn(self):

        # We can parse our custom URNs back into identifiers.
        identifier = self._identifier()
        self._db.commit()
        new_identifier, ignore = Identifier.parse_urn(self._db, identifier.urn)
        eq_(identifier, new_identifier)

        # We can parse urn:isbn URNs into ISBN identifiers. ISBN-10s are
        # converted to ISBN-13s.
        identifier, ignore = Identifier.for_foreign_id(
            self._db, Identifier.ISBN, "9781449358068")
        isbn_urn = "urn:isbn:1449358063"
        isbn_identifier, ignore = Identifier.parse_urn(self._db, isbn_urn)
        eq_(Identifier.ISBN, isbn_identifier.type)
        eq_("9781449358068", isbn_identifier.identifier)

        isbn_urn = "urn:isbn:9781449358068"
        isbn_identifier2, ignore = Identifier.parse_urn(self._db, isbn_urn)
        eq_(isbn_identifier2, isbn_identifier)

        # We can parse ordinary http: or https: URLs into URI
        # identifiers.
        http_identifier, ignore = Identifier.parse_urn(
            self._db, "http://example.com")
        eq_(Identifier.URI, http_identifier.type)
        eq_("http://example.com", http_identifier.identifier)

        https_identifier, ignore = Identifier.parse_urn(
            self._db, "https://example.com")
        eq_(Identifier.URI, https_identifier.type)
        eq_("https://example.com", https_identifier.identifier)

        # We can parse UUIDs.
        uuid_identifier, ignore = Identifier.parse_urn(
            self._db, "urn:uuid:04377e87-ab69-41c8-a2a4-812d55dc0952")
        eq_(Identifier.URI, uuid_identifier.type)
        eq_("urn:uuid:04377e87-ab69-41c8-a2a4-812d55dc0952", uuid_identifier.identifier)

        # A URN we can't handle raises an exception.
        ftp_urn = "ftp://example.com"
        assert_raises(ValueError, Identifier.parse_urn, self._db, ftp_urn)

        # An invalid ISBN raises an exception.
        assert_raises(ValueError, Identifier.parse_urn, self._db, "urn:isbn:notanisbn")

        # Pass in None and you get None.
        eq_(None, Identifier.parse_urn(self._db, None))

    def parse_urn_must_support_license_pools(self):
        # We have no way of associating ISBNs with license pools.
        # If we try to parse an ISBN URN in a context that only accepts
        # URNs that can have associated license pools, we get an exception.
        isbn_urn = "urn:isbn:1449358063"
        assert_raises(
            Identifier.UnresolvableIdentifierException,
            Identifier.parse_urn, self._db, isbn_urn,
            must_support_license_pools=True)

    def test_recursively_equivalent_identifier_ids(self):
        identifier = self._identifier()
        data_source = DataSource.lookup(self._db, DataSource.MANUAL)

        strong_equivalent = self._identifier()
        identifier.equivalent_to(data_source, strong_equivalent, 0.9)

        weak_equivalent = self._identifier()
        identifier.equivalent_to(data_source, weak_equivalent, 0.2)

        level_2_equivalent = self._identifier()
        strong_equivalent.equivalent_to(data_source, level_2_equivalent, 0.5)

        level_3_equivalent = self._identifier()
        level_2_equivalent.equivalent_to(data_source, level_3_equivalent, 0.9)

        level_4_equivalent = self._identifier()
        level_3_equivalent.equivalent_to(data_source, level_4_equivalent, 0.6)

        unrelated = self._identifier()

        # With a low threshold and enough levels, we find all the identifiers.
        equivs = Identifier.recursively_equivalent_identifier_ids(
            self._db, [identifier.id], levels=5, threshold=0.1)
        eq_(set([identifier.id,
                 strong_equivalent.id,
                 weak_equivalent.id,
                 level_2_equivalent.id,
                 level_3_equivalent.id,
                 level_4_equivalent.id]),
            set(equivs[identifier.id]))

        # If we only look at one level, we don't find the level 2, 3, or 4 identifiers.
        equivs = Identifier.recursively_equivalent_identifier_ids(
            self._db, [identifier.id], levels=1, threshold=0.1)
        eq_(set([identifier.id,
                 strong_equivalent.id,
                 weak_equivalent.id]),
            set(equivs[identifier.id]))

        # If we raise the threshold, we don't find the weak identifier.
        equivs = Identifier.recursively_equivalent_identifier_ids(
            self._db, [identifier.id], levels=1, threshold=0.4)
        eq_(set([identifier.id,
                 strong_equivalent.id]),
            set(equivs[identifier.id]))

        # For deeper levels, the strength is the product of the strengths
        # of all the equivalencies in between the two identifiers.

        # In this example:
        # identifier - level_2_equivalent = 0.9 * 0.5 = 0.45
        # identifier - level_3_equivalent = 0.9 * 0.5 * 0.9 = 0.405
        # identifier - level_4_equivalent = 0.9 * 0.5 * 0.9 * 0.6 = 0.243

        # With a threshold of 0.5, level 2 and all subsequent levels are too weak.
        equivs = Identifier.recursively_equivalent_identifier_ids(
            self._db, [identifier.id], levels=5, threshold=0.5)
        eq_(set([identifier.id,
                 strong_equivalent.id]),
            set(equivs[identifier.id]))

        # With a threshold of 0.25, level 2 is strong enough, but level
        # 4 is too weak.
        equivs = Identifier.recursively_equivalent_identifier_ids(
            self._db, [identifier.id], levels=5, threshold=0.25)
        eq_(set([identifier.id,
                 strong_equivalent.id,
                 level_2_equivalent.id,
                 level_3_equivalent.id]),
            set(equivs[identifier.id]))

        # It also works if we start from other identifiers.
        equivs = Identifier.recursively_equivalent_identifier_ids(
            self._db, [strong_equivalent.id], levels=5, threshold=0.1)
        eq_(set([identifier.id,
                 strong_equivalent.id,
                 weak_equivalent.id,
                 level_2_equivalent.id,
                 level_3_equivalent.id,
                 level_4_equivalent.id]),
            set(equivs[strong_equivalent.id]))

        equivs = Identifier.recursively_equivalent_identifier_ids(
            self._db, [level_4_equivalent.id], levels=5, threshold=0.1)
        eq_(set([identifier.id,
                 strong_equivalent.id,
                 level_2_equivalent.id,
                 level_3_equivalent.id,
                 level_4_equivalent.id]),
            set(equivs[level_4_equivalent.id]))

        equivs = Identifier.recursively_equivalent_identifier_ids(
            self._db, [level_4_equivalent.id], levels=5, threshold=0.5)
        eq_(set([level_2_equivalent.id,
                 level_3_equivalent.id,
                 level_4_equivalent.id]),
            set(equivs[level_4_equivalent.id]))

        # A chain of very strong equivalents can keep a high strength
        # even at deep levels. This wouldn't work if we changed the strength
        # threshold by level instead of accumulating a strength product.
        another_identifier = self._identifier()
        l2 = self._identifier()
        l3 = self._identifier()
        l4 = self._identifier()
        l2.equivalent_to(data_source, another_identifier, 1)
        l3.equivalent_to(data_source, l2, 1)
        l4.equivalent_to(data_source, l3, 0.9)
        equivs = Identifier.recursively_equivalent_identifier_ids(
            self._db, [another_identifier.id], levels=5, threshold=0.89)
        eq_(set([another_identifier.id,
                 l2.id,
                 l3.id,
                 l4.id]),
            set(equivs[another_identifier.id]))

        # We can look for multiple identifiers at once.
        equivs = Identifier.recursively_equivalent_identifier_ids(
            self._db, [identifier.id, level_3_equivalent.id], levels=2, threshold=0.8)
        eq_(set([identifier.id,
                 strong_equivalent.id]),
            set(equivs[identifier.id]))
        eq_(set([level_2_equivalent.id,
                 level_3_equivalent.id]),
            set(equivs[level_3_equivalent.id]))

        # The query uses the same db function, but returns equivalents
        # for all identifiers together so it can be used as a subquery.
        query = Identifier.recursively_equivalent_identifier_ids_query(
            Identifier.id, levels=5, threshold=0.1)
        query = query.where(Identifier.id==identifier.id)
        results = self._db.execute(query)
        equivalent_ids = [r[0] for r in results]
        eq_(set([identifier.id,
                 strong_equivalent.id,
                 weak_equivalent.id,
                 level_2_equivalent.id,
                 level_3_equivalent.id,
                 level_4_equivalent.id]),
            set(equivalent_ids))

        query = Identifier.recursively_equivalent_identifier_ids_query(
            Identifier.id, levels=2, threshold=0.8)
        query = query.where(Identifier.id.in_([identifier.id, level_3_equivalent.id]))
        results = self._db.execute(query)
        equivalent_ids = [r[0] for r in results]
        eq_(set([identifier.id,
                 strong_equivalent.id,
                 level_2_equivalent.id,
                 level_3_equivalent.id]),
            set(equivalent_ids))

    def test_licensed_through_collection(self):
        c1 = self._default_collection
        c2 = self._collection()
        c3 = self._collection()

        edition, lp1 = self._edition(collection=c1, with_license_pool=True)
        lp2 = self._licensepool(collection=c2, edition=edition)

        identifier = lp1.identifier
        eq_(lp2.identifier, identifier)

        eq_(lp1, identifier.licensed_through_collection(c1))
        eq_(lp2, identifier.licensed_through_collection(c2))
        eq_(None, identifier.licensed_through_collection(c3))

    def test_missing_coverage_from(self):
        gutenberg = DataSource.lookup(self._db, DataSource.GUTENBERG)
        oclc = DataSource.lookup(self._db, DataSource.OCLC)
        web = DataSource.lookup(self._db, DataSource.WEB)

        # Here are two Gutenberg records.
        g1, ignore = Edition.for_foreign_id(
            self._db, gutenberg, Identifier.GUTENBERG_ID, "1")

        g2, ignore = Edition.for_foreign_id(
            self._db, gutenberg, Identifier.GUTENBERG_ID, "2")

        # One of them has coverage from OCLC Classify
        c1 = self._coverage_record(g1, oclc)

        # The other has coverage from a specific operation on OCLC Classify
        c2 = self._coverage_record(g2, oclc, "some operation")

        # Here's a web record, just sitting there.
        w, ignore = Edition.for_foreign_id(
            self._db, web, Identifier.URI, "http://www.foo.com/")

        # If we run missing_coverage_from we pick up the Gutenberg
        # record with no generic OCLC coverage. It doesn't pick up the
        # other Gutenberg record, it doesn't pick up the web record,
        # and it doesn't pick up the OCLC coverage for a specific
        # operation.
        [in_gutenberg_but_not_in_oclc] = Identifier.missing_coverage_from(
            self._db, [Identifier.GUTENBERG_ID], oclc).all()

        eq_(g2.primary_identifier, in_gutenberg_but_not_in_oclc)

        # If we ask about a specific operation, we get the Gutenberg
        # record that has coverage for that operation, but not the one
        # that has generic OCLC coverage.

        [has_generic_coverage_only] = Identifier.missing_coverage_from(
            self._db, [Identifier.GUTENBERG_ID], oclc, "some operation").all()
        eq_(g1.primary_identifier, has_generic_coverage_only)

        # We don't put web sites into OCLC, so this will pick up the
        # web record (but not the Gutenberg record).
        [in_web_but_not_in_oclc] = Identifier.missing_coverage_from(
            self._db, [Identifier.URI], oclc).all()
        eq_(w.primary_identifier, in_web_but_not_in_oclc)

        # We don't use the web as a source of coverage, so this will
        # return both Gutenberg records (but not the web record).
        eq_([g1.primary_identifier.id, g2.primary_identifier.id], sorted(
            [x.id for x in Identifier.missing_coverage_from(
                self._db, [Identifier.GUTENBERG_ID], web)])
        )

    def test_missing_coverage_from_with_collection(self):
        gutenberg = DataSource.lookup(self._db, DataSource.GUTENBERG)
        identifier = self._identifier()
        collection1 = self._default_collection
        collection2 = self._collection()
        self._coverage_record(identifier, gutenberg, collection=collection1)

        # The Identifier has coverage in collection 1.
        eq_([],
            Identifier.missing_coverage_from(
                self._db, [identifier.type], gutenberg, collection=collection1
            ).all()
        )

        # It is missing coverage in collection 2.
        eq_(
            [identifier], Identifier.missing_coverage_from(
                self._db, [identifier.type], gutenberg, collection=collection2
            ).all()
        )

        # If no collection is specified, we look for a CoverageRecord
        # that also has no collection specified, and the Identifier is
        # not treated as covered.
        eq_([identifier],
            Identifier.missing_coverage_from(
                self._db, [identifier.type], gutenberg
            ).all()
        )


    def test_missing_coverage_from_with_cutoff_date(self):
        gutenberg = DataSource.lookup(self._db, DataSource.GUTENBERG)
        oclc = DataSource.lookup(self._db, DataSource.OCLC)
        web = DataSource.lookup(self._db, DataSource.WEB)

        # Here's an Edition with a coverage record from OCLC classify.
        gutenberg, ignore = Edition.for_foreign_id(
            self._db, gutenberg, Identifier.GUTENBERG_ID, "1")
        identifier = gutenberg.primary_identifier
        oclc = DataSource.lookup(self._db, DataSource.OCLC)
        coverage = self._coverage_record(gutenberg, oclc)

        # The CoverageRecord knows when the coverage was provided.
        timestamp = coverage.timestamp

        # If we ask for Identifiers that are missing coverage records
        # as of that time, we see nothing.
        eq_(
            [],
            Identifier.missing_coverage_from(
                self._db, [identifier.type], oclc,
                count_as_missing_before=timestamp
            ).all()
        )

        # But if we give a time one second later, the Identifier is
        # missing coverage.
        eq_(
            [identifier],
            Identifier.missing_coverage_from(
                self._db, [identifier.type], oclc,
                count_as_missing_before=timestamp+datetime.timedelta(seconds=1)
            ).all()
        )

    def test_opds_entry(self):
        identifier = self._identifier()
        source = DataSource.lookup(self._db, DataSource.CONTENT_CAFE)

        summary = identifier.add_link(
            Hyperlink.DESCRIPTION, 'http://description', source,
            media_type=Representation.TEXT_PLAIN, content='a book'
        )[0]
        cover = identifier.add_link(
            Hyperlink.IMAGE, 'http://cover', source,
            media_type=Representation.JPEG_MEDIA_TYPE
        )[0]

        def get_entry_dict(entry):
            return feedparser.parse(unicode(etree.tostring(entry))).entries[0]

        def format_timestamp(timestamp):
            return timestamp.strftime('%Y-%m-%dT%H:%M:%SZ')

        # The entry includes the urn, description, and cover link.
        entry = get_entry_dict(identifier.opds_entry())
        eq_(identifier.urn, entry.id)
        eq_('a book', entry.summary)
        [cover_link] = entry.links
        eq_('http://cover', cover_link.href)

        # The 'updated' time is set to the latest timestamp associated
        # with the Identifier.
        eq_([], identifier.coverage_records)

        # This may be the time the cover image was mirrored.
        cover.resource.representation.set_as_mirrored(self._url)
        now = datetime.datetime.utcnow()
        cover.resource.representation.mirrored_at = now
        entry = get_entry_dict(identifier.opds_entry())
        eq_(format_timestamp(now), entry.updated)

        # Or it may be a timestamp on a coverage record associated
        # with the Identifier.

        # For whatever reason, this coverage record is missing its
        # timestamp. This indicates an error elsewhere, but it
        # doesn't crash the method we're testing.
        no_timestamp = self._coverage_record(
            identifier, source, operation="bad operation"
        )
        no_timestamp.timestamp = None

        # If a coverage record is dated after the cover image's mirror
        # time, That becomes the new updated time.
        record = self._coverage_record(identifier, source)
        the_future = now + datetime.timedelta(minutes=60)
        record.timestamp = the_future
        identifier.opds_entry()
        entry = get_entry_dict(identifier.opds_entry())
        eq_(format_timestamp(record.timestamp), entry.updated)

        # Basically the latest date is taken from either a coverage record
        # or a representation.
        even_later = now + datetime.timedelta(minutes=120)
        thumbnail = identifier.add_link(
            Hyperlink.THUMBNAIL_IMAGE, 'http://thumb', source,
            media_type=Representation.JPEG_MEDIA_TYPE
        )[0]
        thumb_rep = thumbnail.resource.representation
        cover_rep = cover.resource.representation
        thumbnail.resource.representation.thumbnail_of_id = cover_rep.id
        cover_rep.thumbnails.append(thumb_rep)
        thumbnail.resource.representation.mirrored_at = even_later

        entry = get_entry_dict(identifier.opds_entry())
        # The thumbnail has been added to the links.
        eq_(2, len(entry.links))
        assert any(filter(lambda l: l.href=='http://thumb', entry.links))
        # And the updated time has been changed accordingly.
        expected = thumbnail.resource.representation.mirrored_at
        eq_(format_timestamp(even_later), entry.updated)

class TestEdition(DatabaseTest):

    def test_license_pools(self):
        # Here are two collections that provide access to the same book.
        c1 = self._collection()
        c2 = self._collection()

        edition, lp1 = self._edition(with_license_pool=True)
        lp2 = self._licensepool(edition=edition, collection=c2)

        # Two LicensePools for the same work.
        eq_(lp1.identifier, lp2.identifier)

        # Edition.license_pools contains both.
        eq_(set([lp1, lp2]), set(edition.license_pools))

    def test_author_contributors(self):
        data_source = DataSource.lookup(self._db, DataSource.GUTENBERG)
        id = self._str
        type = Identifier.GUTENBERG_ID

        edition, was_new = Edition.for_foreign_id(
            self._db, data_source, type, id
        )

        # We've listed the same person as primary author and author.
        [alice], ignore = Contributor.lookup(self._db, "Adder, Alice")
        edition.add_contributor(
            alice, [Contributor.AUTHOR_ROLE, Contributor.PRIMARY_AUTHOR_ROLE]
        )

        # We've listed a different person as illustrator.
        [bob], ignore = Contributor.lookup(self._db, "Bitshifter, Bob")
        edition.add_contributor(bob, [Contributor.ILLUSTRATOR_ROLE])

        # Both contributors show up in .contributors.
        eq_(set([alice, bob]), edition.contributors)

        # Only the author shows up in .author_contributors, and she
        # only shows up once.
        eq_([alice], edition.author_contributors)

    def test_for_foreign_id(self):
        """Verify we can get a data source's view of a foreign id."""
        data_source = DataSource.lookup(self._db, DataSource.GUTENBERG)
        id = "549"
        type = Identifier.GUTENBERG_ID

        record, was_new = Edition.for_foreign_id(
            self._db, data_source, type, id)
        eq_(data_source, record.data_source)
        identifier = record.primary_identifier
        eq_(id, identifier.identifier)
        eq_(type, identifier.type)
        eq_(True, was_new)
        eq_([identifier], record.equivalent_identifiers())

        # We can get the same work record by providing only the name
        # of the data source.
        record, was_new = Edition.for_foreign_id(
            self._db, DataSource.GUTENBERG, type, id)
        eq_(data_source, record.data_source)
        eq_(identifier, record.primary_identifier)
        eq_(False, was_new)

    def test_missing_coverage_from(self):
        gutenberg = DataSource.lookup(self._db, DataSource.GUTENBERG)
        oclc = DataSource.lookup(self._db, DataSource.OCLC)
        web = DataSource.lookup(self._db, DataSource.WEB)

        # Here are two Gutenberg records.
        g1, ignore = Edition.for_foreign_id(
            self._db, gutenberg, Identifier.GUTENBERG_ID, "1")

        g2, ignore = Edition.for_foreign_id(
            self._db, gutenberg, Identifier.GUTENBERG_ID, "2")

        # One of them has coverage from OCLC Classify
        c1 = self._coverage_record(g1, oclc)

        # The other has coverage from a specific operation on OCLC Classify
        c2 = self._coverage_record(g2, oclc, "some operation")

        # Here's a web record, just sitting there.
        w, ignore = Edition.for_foreign_id(
            self._db, web, Identifier.URI, "http://www.foo.com/")

        # missing_coverage_from picks up the Gutenberg record with no
        # coverage from OCLC. It doesn't pick up the other
        # Gutenberg record, and it doesn't pick up the web record.
        [in_gutenberg_but_not_in_oclc] = Edition.missing_coverage_from(
            self._db, gutenberg, oclc).all()

        eq_(g2, in_gutenberg_but_not_in_oclc)

        # If we ask about a specific operation, we get the Gutenberg
        # record that has coverage for that operation, but not the one
        # that has generic OCLC coverage.
        [has_generic_coverage_only] = Edition.missing_coverage_from(
            self._db, gutenberg, oclc, "some operation").all()
        eq_(g1, has_generic_coverage_only)

        # We don't put web sites into OCLC, so this will pick up the
        # web record (but not the Gutenberg record).
        [in_web_but_not_in_oclc] = Edition.missing_coverage_from(
            self._db, web, oclc).all()
        eq_(w, in_web_but_not_in_oclc)

        # We don't use the web as a source of coverage, so this will
        # return both Gutenberg records (but not the web record).
        eq_([g1.id, g2.id], sorted([x.id for x in Edition.missing_coverage_from(
            self._db, gutenberg, web)]))

    def test_sort_by_priority(self):
        edition_admin = self._edition(data_source_name=DataSource.LIBRARY_STAFF, with_license_pool=False)
        edition_od, pool = self._edition(data_source_name=DataSource.OVERDRIVE, with_license_pool=True)
        edition_mw = self._edition(data_source_name=DataSource.METADATA_WRANGLER, with_license_pool=False)

        # Unset edition_no_data_source's data source
        edition_no_data_source = self._edition(with_license_pool=False)
        edition_no_data_source.data_source = None

        editions_correct = (edition_no_data_source, edition_od, edition_mw, edition_admin)

        # Give all the editions the same identifier and sort them.
        identifier = pool.identifier
        for edition in editions_correct:
            edition.primary_identifier = identifier
        editions_contender = Edition.sort_by_priority(identifier.primarily_identifies)

        eq_(len(editions_correct), len(editions_contender))
        for index, edition in enumerate(editions_correct):
            eq_(editions_contender[index].title, edition.title)

    def test_equivalent_identifiers(self):

        edition = self._edition()
        identifier = self._identifier()
        data_source = DataSource.lookup(self._db, DataSource.OCLC)

        identifier.equivalent_to(data_source, edition.primary_identifier, 0.6)

        eq_(set([identifier, edition.primary_identifier]),
            set(edition.equivalent_identifiers(threshold=0.5)))

        eq_(set([edition.primary_identifier]),
            set(edition.equivalent_identifiers(threshold=0.7)))

    def test_recursive_edition_equivalence(self):

        # Here's a Edition for a Project Gutenberg text.
        gutenberg, gutenberg_pool = self._edition(
            data_source_name=DataSource.GUTENBERG,
            identifier_type=Identifier.GUTENBERG_ID,
            identifier_id="1",
            with_open_access_download=True,
            title="Original Gutenberg text")

        # Here's a Edition for an Open Library text.
        open_library, open_library_pool = self._edition(
            data_source_name=DataSource.OPEN_LIBRARY,
            identifier_type=Identifier.OPEN_LIBRARY_ID,
            identifier_id="W1111",
            with_open_access_download=True,
            title="Open Library record")

        # We've learned from OCLC Classify that the Gutenberg text is
        # equivalent to a certain OCLC Number. We've learned from OCLC
        # Linked Data that the Open Library text is equivalent to the
        # same OCLC Number.
        oclc_classify = DataSource.lookup(self._db, DataSource.OCLC)
        oclc_linked_data = DataSource.lookup(self._db, DataSource.OCLC_LINKED_DATA)

        oclc_number, ignore = Identifier.for_foreign_id(
            self._db, Identifier.OCLC_NUMBER, "22")
        gutenberg.primary_identifier.equivalent_to(
            oclc_classify, oclc_number, 1)
        open_library.primary_identifier.equivalent_to(
            oclc_linked_data, oclc_number, 1)

        # Here's a Edition for a Recovering the Classics cover.
        web_source = DataSource.lookup(self._db, DataSource.WEB)
        recovering, ignore = Edition.for_foreign_id(
            self._db, web_source, Identifier.URI,
            "http://recoveringtheclassics.com/pride-and-prejudice.jpg")
        recovering.title = "Recovering the Classics cover"

        # We've manually associated that Edition's URI directly
        # with the Project Gutenberg text.
        manual = DataSource.lookup(self._db, DataSource.MANUAL)
        gutenberg.primary_identifier.equivalent_to(
            manual, recovering.primary_identifier, 1)

        # Finally, here's a completely unrelated Edition, which
        # will not be showing up.
        gutenberg2, gutenberg2_pool = self._edition(
            data_source_name=DataSource.GUTENBERG,
            identifier_type=Identifier.GUTENBERG_ID,
            identifier_id="2",
            with_open_access_download=True,
            title="Unrelated Gutenberg record.")

        # When we call equivalent_editions on the Project Gutenberg
        # Edition, we get three Editions: the Gutenberg record
        # itself, the Open Library record, and the Recovering the
        # Classics record.
        #
        # We get the Open Library record because it's associated with
        # the same OCLC Number as the Gutenberg record. We get the
        # Recovering the Classics record because it's associated
        # directly with the Gutenberg record.
        results = list(gutenberg.equivalent_editions())
        eq_(3, len(results))
        assert gutenberg in results
        assert open_library in results
        assert recovering in results

        # Here's a Work that incorporates one of the Gutenberg records.
        work = self._work()
        work.license_pools.extend([gutenberg2_pool])

        # Its set-of-all-editions contains only one record.
        eq_(1, work.all_editions().count())

        # If we add the other Gutenberg record to it, then its
        # set-of-all-editions is extended by that record, *plus*
        # all the Editions equivalent to that record.
        work.license_pools.extend([gutenberg_pool])
        eq_(4, work.all_editions().count())

    def test_calculate_presentation_title(self):
        wr = self._edition(title="The Foo")
        wr.calculate_presentation()
        eq_("Foo, The", wr.sort_title)

        wr = self._edition(title="A Foo")
        wr.calculate_presentation()
        eq_("Foo, A", wr.sort_title)

    def test_calculate_presentation_missing_author(self):
        wr = self._edition()
        self._db.delete(wr.contributions[0])
        self._db.commit()
        wr.calculate_presentation()
        eq_(u"[Unknown]", wr.sort_author)
        eq_(u"[Unknown]", wr.author)

    def test_calculate_presentation_author(self):
        bob, ignore = self._contributor(sort_name="Bitshifter, Bob")
        wr = self._edition(authors=bob.sort_name)
        wr.calculate_presentation()
        eq_("Bob Bitshifter", wr.author)
        eq_("Bitshifter, Bob", wr.sort_author)

        bob.display_name="Bob A. Bitshifter"
        wr.calculate_presentation()
        eq_("Bob A. Bitshifter", wr.author)
        eq_("Bitshifter, Bob", wr.sort_author)

        kelly, ignore = self._contributor(sort_name="Accumulator, Kelly")
        wr.add_contributor(kelly, Contributor.AUTHOR_ROLE)
        wr.calculate_presentation()
        eq_("Kelly Accumulator, Bob A. Bitshifter", wr.author)
        eq_("Accumulator, Kelly ; Bitshifter, Bob", wr.sort_author)

    def test_set_summary(self):
        e, pool = self._edition(with_license_pool=True)
        work = self._work(presentation_edition=e)
        overdrive = DataSource.lookup(self._db, DataSource.OVERDRIVE)

        # Set the work's summmary.
        l1, new = pool.add_link(Hyperlink.DESCRIPTION, None, overdrive, "text/plain",
                      "F")
        work.set_summary(l1.resource)

        eq_(l1.resource, work.summary)
        eq_("F", work.summary_text)

        # Remove the summary.
        work.set_summary(None)

        eq_(None, work.summary)
        eq_("", work.summary_text)

    def test_calculate_evaluate_summary_quality_with_privileged_data_sources(self):
        e, pool = self._edition(with_license_pool=True)
        oclc = DataSource.lookup(self._db, DataSource.OCLC_LINKED_DATA)
        overdrive = DataSource.lookup(self._db, DataSource.OVERDRIVE)

        # There's a perfunctory description from Overdrive.
        l1, new = pool.add_link(Hyperlink.SHORT_DESCRIPTION, None, overdrive, "text/plain",
                      "F")

        overdrive_resource = l1.resource

        # There's a much better description from OCLC Linked Data.
        l2, new = pool.add_link(Hyperlink.DESCRIPTION, None, oclc, "text/plain",
                      """Nothing about working with his former high school crush, Stephanie Stephens, is ideal. Still, if Aaron Caruthers intends to save his grandmother's bakery, he must. Good thing he has a lot of ideas he can't wait to implement. He never imagines Stephanie would have her own ideas for the business. Or that they would clash with his!""")
        oclc_resource = l2.resource

        # In a head-to-head evaluation, the OCLC Linked Data description wins.
        ids = [e.primary_identifier.id]
        champ1, resources = Identifier.evaluate_summary_quality(self._db, ids)

        eq_(set([overdrive_resource, oclc_resource]), set(resources))
        eq_(oclc_resource, champ1)

        # But if we say that Overdrive is the privileged data source, it wins
        # automatically. The other resource isn't even considered.
        champ2, resources2 = Identifier.evaluate_summary_quality(
            self._db, ids, [overdrive])
        eq_(overdrive_resource, champ2)
        eq_([overdrive_resource], resources2)

        # If we say that some other data source is privileged, and
        # there are no descriptions from that data source, a
        # head-to-head evaluation is performed, and OCLC Linked Data
        # wins.
        threem = DataSource.lookup(self._db, DataSource.THREEM)
        champ3, resources3 = Identifier.evaluate_summary_quality(
            self._db, ids, [threem])
        eq_(set([overdrive_resource, oclc_resource]), set(resources3))
        eq_(oclc_resource, champ3)

        # If there are two privileged data sources and there's no
        # description from the first, the second is used.
        champ4, resources4 = Identifier.evaluate_summary_quality(
            self._db, ids, [threem, overdrive])
        eq_([overdrive_resource], resources4)
        eq_(overdrive_resource, champ4)

        # Even an empty string wins if it's from the most privileged data source.
        # This is not a silly example.  The librarian may choose to set the description
        # to an empty string in the admin inteface, to override a bad overdrive/etc. description.
        staff = DataSource.lookup(self._db, DataSource.LIBRARY_STAFF)
        l3, new = pool.add_link(Hyperlink.SHORT_DESCRIPTION, None, staff, "text/plain", "")
        staff_resource = l3.resource

        champ5, resources5 = Identifier.evaluate_summary_quality(
            self._db, ids, [staff, overdrive])
        eq_([staff_resource], resources5)
        eq_(staff_resource, champ5)

    def test_calculate_presentation_cover(self):
        # Here's a cover image with a thumbnail.
        representation, ignore = get_one_or_create(self._db, Representation, url="http://cover")
        representation.media_type = Representation.JPEG_MEDIA_TYPE
        representation.mirrored_at = datetime.datetime.now()
        representation.mirror_url = "http://mirror/cover"
        thumb, ignore = get_one_or_create(self._db, Representation, url="http://thumb")
        thumb.media_type = Representation.JPEG_MEDIA_TYPE
        thumb.mirrored_at = datetime.datetime.now()
        thumb.mirror_url = "http://mirror/thumb"
        thumb.thumbnail_of_id = representation.id

        # Verify that a cover for the edition's primary identifier is used.
        e, pool = self._edition(with_license_pool=True)
        link, ignore = e.primary_identifier.add_link(Hyperlink.IMAGE, "http://cover", e.data_source)
        link.resource.representation = representation
        e.calculate_presentation()
        eq_("http://mirror/cover", e.cover_full_url)
        eq_("http://mirror/thumb", e.cover_thumbnail_url)

        # Verify that a cover will be used even if it's some
        # distance away along the identifier-equivalence line.
        e, pool = self._edition(with_license_pool=True)
        oclc_classify = DataSource.lookup(self._db, DataSource.OCLC)
        oclc_number, ignore = Identifier.for_foreign_id(
            self._db, Identifier.OCLC_NUMBER, "22")
        e.primary_identifier.equivalent_to(
            oclc_classify, oclc_number, 1)
        link, ignore = oclc_number.add_link(Hyperlink.IMAGE, "http://cover", oclc_classify)
        link.resource.representation = representation
        e.calculate_presentation()
        eq_("http://mirror/cover", e.cover_full_url)
        eq_("http://mirror/thumb", e.cover_thumbnail_url)

        # Verify that a nearby cover takes precedence over a
        # faraway cover.
        link, ignore = e.primary_identifier.add_link(Hyperlink.IMAGE, "http://nearby-cover", e.data_source)
        nearby, ignore = get_one_or_create(self._db, Representation, url=link.resource.url)
        nearby.media_type = Representation.JPEG_MEDIA_TYPE
        nearby.mirrored_at = datetime.datetime.now()
        nearby.mirror_url = "http://mirror/nearby-cover"
        link.resource.representation = nearby
        nearby_thumb, ignore = get_one_or_create(self._db, Representation, url="http://nearby-thumb")
        nearby_thumb.media_type = Representation.JPEG_MEDIA_TYPE
        nearby_thumb.mirrored_at = datetime.datetime.now()
        nearby_thumb.mirror_url = "http://mirror/nearby-thumb"
        nearby_thumb.thumbnail_of_id = nearby.id
        e.calculate_presentation()
        eq_("http://mirror/nearby-cover", e.cover_full_url)
        eq_("http://mirror/nearby-thumb", e.cover_thumbnail_url)

        # Verify that a thumbnail is used even if there's
        # no full-sized cover.
        e, pool = self._edition(with_license_pool=True)
        link, ignore = e.primary_identifier.add_link(Hyperlink.THUMBNAIL_IMAGE, "http://thumb", e.data_source)
        link.resource.representation = thumb
        e.calculate_presentation()
        eq_(None, e.cover_full_url)
        eq_("http://mirror/thumb", e.cover_thumbnail_url)


    def test_calculate_presentation_registers_coverage_records(self):
        edition = self._edition()
        identifier = edition.primary_identifier

        # This Identifier has no CoverageRecords.
        eq_([], identifier.coverage_records)

        # But once we calculate the Edition's presentation...
        edition.calculate_presentation()

        # Two CoverageRecords have been associated with this Identifier.
        records = identifier.coverage_records

        # One for setting the Edition metadata and one for choosing
        # the Edition's cover.
        expect = set([
            CoverageRecord.SET_EDITION_METADATA_OPERATION,
            CoverageRecord.CHOOSE_COVER_OPERATION]
        )
        eq_(expect, set([x.operation for x in records]))

        # We know the records are associated with this specific
        # Edition, not just the Identifier, because each
        # CoverageRecord's DataSource is set to this Edition's
        # DataSource.
        eq_(
            [edition.data_source, edition.data_source],
            [x.data_source for x in records]
        )

    def test_no_permanent_work_id_for_edition_with_no_title(self):
        """An edition with no title is not assigned a permanent work ID."""
        edition = self._edition()
        edition.title = ''
        eq_(None, edition.permanent_work_id)
        edition.calculate_permanent_work_id()
        eq_(None, edition.permanent_work_id)
        edition.title = u'something'
        edition.calculate_permanent_work_id()
        assert_not_equal(None, edition.permanent_work_id)

    def test_choose_cover_can_choose_full_image_and_thumbnail_separately(self):
        edition = self._edition()

        # This edition has a full-sized image and a thumbnail image,
        # but there is no evidence that they are the _same_ image.
        main_image, ignore = edition.primary_identifier.add_link(
            Hyperlink.IMAGE, "http://main/",
            edition.data_source, Representation.PNG_MEDIA_TYPE
        )
        thumbnail_image, ignore = edition.primary_identifier.add_link(
            Hyperlink.THUMBNAIL_IMAGE, "http://thumbnail/",
            edition.data_source, Representation.PNG_MEDIA_TYPE
        )

        # Nonetheless, Edition.choose_cover() will assign the
        # potentially unrelated images to the Edition, because there
        # is no better option.
        edition.choose_cover()
        eq_(main_image.resource.url, edition.cover_full_url)
        eq_(thumbnail_image.resource.url, edition.cover_thumbnail_url)

        # If there is a clear indication that one of the thumbnails
        # associated with the identifier is a thumbnail _of_ the
        # full-sized image...
        thumbnail_2, ignore = edition.primary_identifier.add_link(
            Hyperlink.THUMBNAIL_IMAGE, "http://thumbnail2/",
            edition.data_source, Representation.PNG_MEDIA_TYPE
        )
        thumbnail_2.resource.representation.thumbnail_of = main_image.resource.representation
        edition.choose_cover()

        # ...That thumbnail will be chosen in preference to the
        # possibly unrelated thumbnail.
        eq_(main_image.resource.url, edition.cover_full_url)
        eq_(thumbnail_2.resource.url, edition.cover_thumbnail_url)


class TestContributor(DatabaseTest):

    def test_marc_code_for_every_role_constant(self):
        """We have determined the MARC Role Code for every role
        that's important enough we gave it a constant in the Contributor
        class.
        """
        for constant, value in Contributor.__dict__.items():
            if not constant.endswith('_ROLE'):
                # Not a constant.
                continue
            assert value in Contributor.MARC_ROLE_CODES

    def test_lookup_by_viaf(self):

        # Two contributors named Bob.
        bob1, new = Contributor.lookup(self._db, sort_name="Bob", viaf="foo")
        bob2, new = Contributor.lookup(self._db, sort_name="Bob", viaf="bar")

        assert bob1 != bob2

        eq_((bob1, False), Contributor.lookup(self._db, viaf="foo"))

    def test_lookup_by_lc(self):

        # Two contributors named Bob.
        bob1, new = Contributor.lookup(self._db, sort_name="Bob", lc="foo")
        bob2, new = Contributor.lookup(self._db, sort_name="Bob", lc="bar")

        assert bob1 != bob2

        eq_((bob1, False), Contributor.lookup(self._db, lc="foo"))

    def test_lookup_by_viaf_interchangeable(self):
        # Two contributors with the same lc. This shouldn't happen, but
        # the reason it shouldn't happen is these two people are the same
        # person, so lookup() should just pick one and go with it.
        bob1, new = self._contributor(sort_name="Bob", lc="foo")
        bob2, new = self._contributor()
        bob2.sort_name = "Bob"
        bob2.lc = "foo"
        self._db.commit()
        assert bob1 != bob2
        [some_bob], new = Contributor.lookup(
            self._db, sort_name="Bob", lc="foo"
        )
        eq_(False, new)
        assert some_bob in (bob1, bob2)

    def test_lookup_by_name(self):

        # Two contributors named Bob.
        bob1, new = Contributor.lookup(self._db, sort_name=u"Bob", lc=u"foo")
        bob2, new = Contributor.lookup(self._db, sort_name=u"Bob", lc=u"bar")

        # Lookup by name finds both of them.
        bobs, new = Contributor.lookup(self._db, sort_name=u"Bob")
        eq_(False, new)
        eq_(["Bob", "Bob"], [x.sort_name for x in bobs])

    def test_create_by_lookup(self):
        [bob1], new = Contributor.lookup(self._db, sort_name=u"Bob")
        eq_("Bob", bob1.sort_name)
        eq_(True, new)

        [bob2], new = Contributor.lookup(self._db, sort_name=u"Bob")
        eq_(bob1, bob2)
        eq_(False, new)

    def test_merge(self):

        # Here's Robert.
        [robert], ignore = Contributor.lookup(self._db, sort_name=u"Robert")

        # Here's Bob.
        [bob], ignore = Contributor.lookup(self._db, sort_name=u"Jones, Bob")
        bob.extra[u'foo'] = u'bar'
        bob.aliases = [u'Bobby']
        bob.viaf = u'viaf'
        bob.lc = u'lc'
        bob.display_name = u"Bob Jones"
        bob.family_name = u"Bobb"
        bob.wikipedia_name = u"Bob_(Person)"

        # Each is a contributor to a Edition.
        data_source = DataSource.lookup(self._db, DataSource.GUTENBERG)

        roberts_book, ignore = Edition.for_foreign_id(
            self._db, data_source, Identifier.GUTENBERG_ID, u"1")
        roberts_book.add_contributor(robert, Contributor.AUTHOR_ROLE)

        bobs_book, ignore = Edition.for_foreign_id(
            self._db, data_source, Identifier.GUTENBERG_ID, u"10")
        bobs_book.add_contributor(bob, Contributor.AUTHOR_ROLE)

        # In a shocking turn of events, it transpires that "Bob" and
        # "Robert" are the same person. We merge "Bob" into Robert
        # thusly:
        bob.merge_into(robert)

        # 'Bob' is now listed as an alias for Robert, as is Bob's
        # alias.
        eq_([u'Jones, Bob', u'Bobby'], robert.aliases)

        # The extra information associated with Bob is now associated
        # with Robert.
        eq_(u'bar', robert.extra['foo'])

        eq_(u"viaf", robert.viaf)
        eq_(u"lc", robert.lc)
        eq_(u"Bobb", robert.family_name)
        eq_(u"Bob Jones", robert.display_name)
        eq_(u"Robert", robert.sort_name)
        eq_(u"Bob_(Person)", robert.wikipedia_name)

        # The standalone 'Bob' record has been removed from the database.
        eq_(
            [],
            self._db.query(Contributor).filter(Contributor.sort_name=="Bob").all())

        # Bob's book is now associated with 'Robert', not the standalone
        # 'Bob' record.
        eq_([robert], bobs_book.author_contributors)

        # confirm the sort_name is propagated, if not already set in the destination contributor
        robert.sort_name = None
        [bob], ignore = Contributor.lookup(self._db, sort_name=u"Jones, Bob")
        bob.merge_into(robert)
        eq_(u"Jones, Bob", robert.sort_name)



    def _names(self, in_name, out_family, out_display,
               default_display_name=None):
        f, d = Contributor._default_names(in_name, default_display_name)
        eq_(f, out_family)
        eq_(d, out_display)

    def test_default_names(self):

        # Pass in a default display name and it will always be used.
        self._names("Jones, Bob", "Jones", "Sally Smith",
                    default_display_name="Sally Smith")

        # Corporate names are untouched and get no family name.
        self._names("Bob's Books.", None, "Bob's Books.")
        self._names("Bob's Books, Inc.", None, "Bob's Books, Inc.")
        self._names("Little, Brown &amp; Co.", None, "Little, Brown & Co.")
        self._names("Philadelphia Broad Street Church (Philadelphia, Pa.)",
                    None, "Philadelphia Broad Street Church")

        # Dates and other gibberish after a name is removed.
        self._names("Twain, Mark, 1855-1910", "Twain", "Mark Twain")
        self._names("Twain, Mark, ???-1910", "Twain", "Mark Twain")
        self._names("Twain, Mark, circ. 1900", "Twain", "Mark Twain")
        self._names("Twain, Mark, !@#!@", "Twain", "Mark Twain")
        self._names(
            "Coolbrith, Ina D. 1842?-1928", "Coolbrith", "Ina D. Coolbrith")
        self._names("Caesar, Julius, 1st cent.", "Caesar", "Julius Caesar")
        self._names("Arrian, 2nd cent.", "Arrian", "Arrian")
        self._names("Hafiz, 14th cent.", "Hafiz", "Hafiz")
        self._names("Hormel, Bob 1950?-", "Hormel", "Bob Hormel")
        self._names("Holland, Henry 1583-1650? Monumenta sepulchraria Sancti Pauli",
                    "Holland", "Henry Holland")


        # Suffixes stay on the end, except for "Mrs.", which goes
        # to the front.
        self._names("Twain, Mark, Jr.", "Twain", "Mark Twain, Jr.")
        self._names("House, Gregory, M.D.", "House", "Gregory House, M.D.")
        self._names("Twain, Mark, Mrs.", "Twain", "Mrs. Mark Twain")
        self._names("Twain, Mark, Mrs", "Twain", "Mrs Mark Twain")

        # The easy case.
        self._names("Twain, Mark", "Twain", "Mark Twain")
        self._names("Geering, R. G.", "Geering", "R. G. Geering")


    def test_sort_name(self):
        bob, new = get_one_or_create(self._db, Contributor, sort_name=None)
        eq_(None, bob.sort_name)

        bob, ignore = self._contributor(sort_name="Bob Bitshifter")
        bob.sort_name = None
        eq_(None, bob.sort_name)

        bob, ignore = self._contributor(sort_name="Bob Bitshifter")
        eq_("Bitshifter, Bob", bob.sort_name)

        bob, ignore = self._contributor(sort_name="Bitshifter, Bob")
        eq_("Bitshifter, Bob", bob.sort_name)

        # test that human name parser doesn't die badly on foreign names
        bob, ignore = self._contributor(sort_name=u"Боб  Битшифтер")
        eq_(u"Битшифтер, Боб", bob.sort_name)
