# encoding: utf-8
import datetime
import os
import random

from nose.tools import (
    assert_raises,
    assert_raises_regexp,
    assert_not_equal,
    eq_,
    set_trace,
)
from psycopg2.extras import NumericRange
import core.model
from core.model.works import (
    Work,
    WorkGenre,
)
from core.external_search import (
    DummyExternalSearchIndex,
)
import core.classifier
from .. import DatabaseTest


class TestWork(DatabaseTest):

    def test_complaints(self):
        work = self._work(with_license_pool=True)

        [lp1] = work.license_pools
        lp2 = self._licensepool(
            edition=work.presentation_edition,
            data_source_name=DataSource.OVERDRIVE
        )
        lp2.work = work

        complaint_type = random.choice(list(Complaint.VALID_TYPES))
        complaint1, ignore = Complaint.register(
            lp1, complaint_type, "blah", "blah"
        )
        complaint2, ignore = Complaint.register(
            lp2, complaint_type, "blah", "blah"
        )

        # Create a complaint with no association with the work.
        _edition, lp3 = self._edition(with_license_pool=True)
        complaint3, ignore = Complaint.register(
            lp3, complaint_type, "blah", "blah"
        )

        # Only the first two complaints show up in work.complaints.
        eq_(sorted([complaint1.id, complaint2.id]),
            sorted([x.id for x in work.complaints]))

    def test_all_identifier_ids(self):
        work = self._work(with_license_pool=True)
        lp = work.license_pools[0]
        identifier = self._identifier()
        data_source = DataSource.lookup(self._db, DataSource.OCLC)
        identifier.equivalent_to(data_source, lp.identifier, 1)

        # Make sure there aren't duplicates in the list, if an
        # identifier's equivalent to two of the primary identifiers.
        lp2 = self._licensepool(None)
        work.license_pools.append(lp2)
        identifier.equivalent_to(data_source, lp2.identifier, 1)

        all_identifier_ids = work.all_identifier_ids()
        eq_(3, len(all_identifier_ids))
        eq_(set([lp.identifier.id, lp2.identifier.id, identifier.id]),
            set(all_identifier_ids))

    def test_from_identifiers(self):
        # Prep a work to be identified and a work to be ignored.
        work = self._work(with_license_pool=True, with_open_access_download=True)
        lp = work.license_pools[0]
        ignored_work = self._work(with_license_pool=True, with_open_access_download=True)

        # No identifiers returns None.
        result = Work.from_identifiers(self._db, [])
        eq_(None, result)

        # A work can be found according to its identifier.
        identifiers = [lp.identifier]
        result = Work.from_identifiers(self._db, identifiers).all()
        eq_(1, len(result))
        eq_([work], result)

        # When the work has an equivalent identifier.
        isbn = self._identifier(Identifier.ISBN)
        source = lp.data_source
        lp.identifier.equivalent_to(source, isbn, 1)

        # It can be found according to that equivalency.
        identifiers = [isbn]
        result = Work.from_identifiers(self._db, identifiers).all()
        eq_(1, len(result))
        eq_([work], result)

        # Unless the strength is too low.
        lp.identifier.equivalencies[0].strength = 0.8
        identifiers = [isbn]

        result = Work.from_identifiers(self._db, identifiers).all()
        eq_([], result)

        # Two+ of the same or equivalent identifiers lead to one result.
        identifiers = [lp.identifier, isbn, lp.identifier]
        result = Work.from_identifiers(self._db, identifiers).all()
        eq_(1, len(result))
        eq_([work], result)

        # It accepts a base query.
        qu = self._db.query(Work).join(LicensePool).join(Identifier).\
            filter(LicensePool.suppressed)
        identifiers = [lp.identifier]
        result = Work.from_identifiers(self._db, identifiers, base_query=qu).all()
        # Because the work's license_pool isn't suppressed, it isn't returned.
        eq_([], result)

        # It's possible to filter a field other than Identifier.id.
        # Here, we filter based on the value of
        # mv_works_for_lanes.identifier_id.
        from model import MaterializedWorkWithGenre as mw
        qu = self._db.query(mw)
        m = lambda: Work.from_identifiers(
            self._db, [lp.identifier], base_query=qu,
            identifier_id_field=mw.identifier_id
        ).all()
        eq_([], m())
        self.add_to_materialized_view([work, ignored_work])
        eq_([work.id], [x.works_id for x in m()])

    def test_calculate_presentation(self):
        # Test that:
        # - work coverage records are made on work creation and primary edition selection.
        # - work's presentation information (author, title, etc. fields) does a proper job
        #   of combining fields from underlying editions.
        # - work's presentation information keeps in sync with work's presentation edition.
        # - there can be only one edition that thinks it's the presentation edition for this work.
        # - time stamps are stamped.
        # - higher-standard sources (library staff) can replace, but not delete, authors.

        gutenberg_source = DataSource.GUTENBERG
        gitenberg_source = DataSource.PROJECT_GITENBERG

        [bob], ignore = Contributor.lookup(self._db, u"Bitshifter, Bob")
        bob.family_name, bob.display_name = bob.default_names()

        edition1, pool1 = self._edition(gitenberg_source, Identifier.GUTENBERG_ID,
            with_license_pool=True, with_open_access_download=True, authors=[])
        edition1.title = u"The 1st Title"
        edition1.subtitle = u"The 1st Subtitle"
        edition1.add_contributor(bob, Contributor.AUTHOR_ROLE)

        edition2, pool2 = self._edition(gitenberg_source, Identifier.GUTENBERG_ID,
            with_license_pool=True, with_open_access_download=True, authors=[])
        edition2.title = u"The 2nd Title"
        edition2.subtitle = u"The 2nd Subtitle"
        edition2.add_contributor(bob, Contributor.AUTHOR_ROLE)
        [alice], ignore = Contributor.lookup(self._db, u"Adder, Alice")
        alice.family_name, alice.display_name = alice.default_names()
        edition2.add_contributor(alice, Contributor.AUTHOR_ROLE)

        edition3, pool3 = self._edition(gutenberg_source, Identifier.GUTENBERG_ID,
            with_license_pool=True, with_open_access_download=True, authors=[])
        edition3.title = u"The 2nd Title"
        edition3.subtitle = u"The 2nd Subtitle"
        edition3.add_contributor(bob, Contributor.AUTHOR_ROLE)
        edition3.add_contributor(alice, Contributor.AUTHOR_ROLE)

        work = self._slow_work(presentation_edition=edition2)
        # add in 3, 2, 1 order to make sure the selection of edition1 as presentation
        # in the second half of the test is based on business logic, not list order.
        for p in pool3, pool1:
            work.license_pools.append(p)

        # The author of the Work is the author of its primary work record.
        eq_("Alice Adder, Bob Bitshifter", work.author)

        # This Work starts out with a single CoverageRecord reflecting the
        # work done to generate its initial OPDS entry, and then it adds choose-edition
        # as a primary edition is set.
        [choose_edition, generate_opds] = sorted(work.coverage_records, key=lambda x: x.operation)
        assert (generate_opds.operation == WorkCoverageRecord.GENERATE_OPDS_OPERATION)
        assert (choose_edition.operation == WorkCoverageRecord.CHOOSE_EDITION_OPERATION)

        # pools aren't yet aware of each other
        eq_(pool1.superceded, False)
        eq_(pool2.superceded, False)
        eq_(pool3.superceded, False)

        work.last_update_time = None
        work.presentation_ready = True
        index = DummyExternalSearchIndex()

        work.calculate_presentation(search_index_client=index)

        # The author of the Work has not changed.
        eq_("Alice Adder, Bob Bitshifter", work.author)

        # one and only one license pool should be un-superceded
        eq_(pool1.superceded, True)
        eq_(pool2.superceded, False)
        eq_(pool3.superceded, True)

        # sanity check
        eq_(work.presentation_edition, pool2.presentation_edition)
        eq_(work.presentation_edition, edition2)

        # editions that aren't the presentation edition have no work
        eq_(edition1.work, None)
        eq_(edition2.work, work)
        eq_(edition3.work, None)

        # The title of the Work is the title of its primary work record.
        eq_("The 2nd Title", work.title)
        eq_("The 2nd Subtitle", work.subtitle)

        # The author of the Work is the author of its primary work record.
        eq_("Alice Adder, Bob Bitshifter", work.author)
        eq_("Adder, Alice ; Bitshifter, Bob", work.sort_author)

        # The last update time has been set.
        # Updating availability also modified work.last_update_time.
        assert (datetime.datetime.utcnow() - work.last_update_time) < datetime.timedelta(seconds=2)

        # The index has not been updated.
        eq_([], index.docs.items())

        # The Work now has a complete set of WorkCoverageRecords
        # associated with it, reflecting all the operations that
        # occured as part of calculate_presentation().
        #
        # All the work has actually been done, except for the work of
        # updating the search index, which has been registered and
        # will be done later.
        records = work.coverage_records

        wcr = WorkCoverageRecord
        success = wcr.SUCCESS
        expect = set([
            (wcr.CHOOSE_EDITION_OPERATION, success),
            (wcr.CLASSIFY_OPERATION, success),
            (wcr.SUMMARY_OPERATION, success),
            (wcr.QUALITY_OPERATION, success),
            (wcr.GENERATE_OPDS_OPERATION, success),
            (wcr.UPDATE_SEARCH_INDEX_OPERATION, wcr.REGISTERED),
        ])
        eq_(expect, set([(x.operation, x.status) for x in records]))

        # Now mark the pool with the presentation edition as suppressed.
        # work.calculate_presentation() will call work.mark_licensepools_as_superceded(),
        # which will mark the suppressed pool as superceded and take its edition out of the running.
        # Make sure that work's presentation edition and work's author, etc.
        # fields are updated accordingly, and that the superceded pool's edition
        # knows it's no longer the champ.
        pool2.suppressed = True

        work.calculate_presentation(search_index_client=index)

        # The title of the Work is the title of its new primary work record.
        eq_("The 1st Title", work.title)
        eq_("The 1st Subtitle", work.subtitle)

        # author of composite edition is now just Bob
        eq_("Bob Bitshifter", work.author)
        eq_("Bitshifter, Bob", work.sort_author)

        # sanity check
        eq_(work.presentation_edition, pool1.presentation_edition)
        eq_(work.presentation_edition, edition1)

        # editions that aren't the presentation edition have no work
        eq_(edition1.work, work)
        eq_(edition2.work, None)
        eq_(edition3.work, None)

        # The last update time has been set.
        # Updating availability also modified work.last_update_time.
        assert (datetime.datetime.utcnow() - work.last_update_time) < datetime.timedelta(seconds=2)

        # make a staff (admin interface) edition.  its fields should supercede all others below it
        # except when it has no contributors, and they do.
        pool2.suppressed = False

        staff_edition = self._edition(data_source_name=DataSource.LIBRARY_STAFF,
            with_license_pool=False, authors=[])
        staff_edition.title = u"The Staff Title"
        staff_edition.primary_identifier = pool2.identifier
        # set edition's authorship to "nope", and make sure the lower-priority
        # editions' authors don't get clobbered
        staff_edition.contributions = []
        staff_edition.author = Edition.UNKNOWN_AUTHOR
        staff_edition.sort_author = Edition.UNKNOWN_AUTHOR

        work.calculate_presentation(search_index_client=index)

        # The title of the Work got superceded.
        eq_("The Staff Title", work.title)

        # The author of the Work is still the author of edition2 and was not clobbered.
        eq_("Alice Adder, Bob Bitshifter", work.author)
        eq_("Adder, Alice ; Bitshifter, Bob", work.sort_author)

    def test_set_presentation_ready(self):

        work = self._work(with_license_pool=True)

        search = DummyExternalSearchIndex()
        # This is how the work will be represented in the dummy search
        # index.
        index_key = (search.works_index,
                     DummyExternalSearchIndex.work_document_type,
                     work.id)

        presentation = work.presentation_edition
        work.set_presentation_ready_based_on_content(search_index_client=search)
        eq_(True, work.presentation_ready)

        # The work has not been added to the search index.
        eq_([], search.docs.keys())

        # But the work of adding it to the search engine has been
        # registered.
        [record] = [
            x for x in work.coverage_records
            if x.operation==WorkCoverageRecord.UPDATE_SEARCH_INDEX_OPERATION
        ]
        eq_(WorkCoverageRecord.REGISTERED, record.status)

        # This work is presentation ready because it has a title

        # Remove the title, and the work stops being presentation
        # ready.
        presentation.title = None
        work.set_presentation_ready_based_on_content(search_index_client=search)
        eq_(False, work.presentation_ready)

        # The work has been removed from the search index.
        eq_([], search.docs.keys())

        # Restore the title, and everything is fixed.
        presentation.title = u"foo"
        work.set_presentation_ready_based_on_content(search_index_client=search)
        eq_(True, work.presentation_ready)

        # Remove the fiction status, and the work is still
        # presentation ready.
        work.fiction = None
        work.set_presentation_ready_based_on_content(search_index_client=search)
        eq_(True, work.presentation_ready)

    def test_assign_genres_from_weights(self):
        work = self._work()

        # This work was once classified under Fantasy and Romance.
        work.assign_genres_from_weights({Romance : 1000, Fantasy : 1000})
        self._db.commit()
        before = sorted((x.genre.name, x.affinity) for x in work.work_genres)
        eq_([(u'Fantasy', 0.5), (u'Romance', 0.5)], before)

        # But now it's classified under Science Fiction and Romance.
        work.assign_genres_from_weights({Romance : 100, Science_Fiction : 300})
        self._db.commit()
        after = sorted((x.genre.name, x.affinity) for x in work.work_genres)
        eq_([(u'Romance', 0.25), (u'Science Fiction', 0.75)], after)

    def test_classifications_with_genre(self):
        work = self._work(with_open_access_download=True)
        identifier = work.presentation_edition.primary_identifier
        genres = self._db.query(Genre).all()
        subject1 = self._subject(type="type1", identifier="subject1")
        subject1.genre = genres[0]
        subject2 = self._subject(type="type2", identifier="subject2")
        subject2.genre = genres[1]
        subject3 = self._subject(type="type2", identifier="subject3")
        subject3.genre = None
        source = DataSource.lookup(self._db, DataSource.AXIS_360)
        classification1 = self._classification(
            identifier=identifier, subject=subject1,
            data_source=source, weight=1)
        classification2 = self._classification(
            identifier=identifier, subject=subject2,
            data_source=source, weight=2)
        classification3 = self._classification(
            identifier=identifier, subject=subject3,
            data_source=source, weight=2)

        results = work.classifications_with_genre().all()

        eq_([classification2, classification1], results)

    def test_mark_licensepools_as_superceded(self):
        # A commercial LP that somehow got superceded will be
        # un-superceded.
        commercial = self._licensepool(
            None, data_source_name=DataSource.OVERDRIVE
        )
        work, is_new = commercial.calculate_work()
        commercial.superceded = True
        work.mark_licensepools_as_superceded()
        eq_(False, commercial.superceded)

        # An open-access LP that was superceded will be un-superceded if
        # chosen.
        gutenberg = self._licensepool(
            None, data_source_name=DataSource.GUTENBERG,
            open_access=True, with_open_access_download=True
        )
        work, is_new = gutenberg.calculate_work()
        gutenberg.superceded = True
        work.mark_licensepools_as_superceded()
        eq_(False, gutenberg.superceded)

        # Of two open-access LPs, the one from the higher-quality data
        # source will be un-superceded, and the one from the
        # lower-quality data source will be superceded.
        standard_ebooks = self._licensepool(
            None, data_source_name=DataSource.STANDARD_EBOOKS,
            open_access=True, with_open_access_download=True
        )
        work.license_pools.append(standard_ebooks)
        gutenberg.superceded = False
        standard_ebooks.superceded = True
        work.mark_licensepools_as_superceded()
        eq_(True, gutenberg.superceded)
        eq_(False, standard_ebooks.superceded)

        # Of three open-access pools, 1 and only 1 will be chosen as non-superceded.
        gitenberg1 = self._licensepool(edition=None, open_access=True,
            data_source_name=DataSource.PROJECT_GITENBERG, with_open_access_download=True
        )

        gitenberg2 = self._licensepool(edition=None, open_access=True,
            data_source_name=DataSource.PROJECT_GITENBERG, with_open_access_download=True
        )

        gutenberg1 = self._licensepool(edition=None, open_access=True,
            data_source_name=DataSource.GUTENBERG, with_open_access_download=True
        )

        work_multipool = self._work(presentation_edition=None)
        work_multipool.license_pools.append(gutenberg1)
        work_multipool.license_pools.append(gitenberg2)
        work_multipool.license_pools.append(gitenberg1)

        # pools aren't yet aware of each other
        eq_(gutenberg1.superceded, False)
        eq_(gitenberg1.superceded, False)
        eq_(gitenberg2.superceded, False)

        # make pools figure out who's best
        work_multipool.mark_licensepools_as_superceded()

        eq_(gutenberg1.superceded, True)
        # There's no way to choose between the two gitenberg pools,
        # so making sure only one has been chosen is enough.
        chosen_count = 0
        for chosen_pool in gutenberg1, gitenberg1, gitenberg2:
            if chosen_pool.superceded is False:
                chosen_count += 1;
        eq_(chosen_count, 1)

        # throw wrench in
        gitenberg1.suppressed = True

        # recalculate bests
        work_multipool.mark_licensepools_as_superceded()
        eq_(gutenberg1.superceded, True)
        eq_(gitenberg1.superceded, True)
        eq_(gitenberg2.superceded, False)

        # A suppressed pool won't be superceded if it's the only pool for a work.
        only_pool = self._licensepool(
            None, open_access=True, with_open_access_download=True
        )
        work, ignore = only_pool.calculate_work()
        only_pool.suppressed = True
        work.mark_licensepools_as_superceded()
        eq_(False, only_pool.superceded)


    def test_work_remains_viable_on_pools_suppressed(self):
        """ If a work has all of its pools suppressed, the work's author, title,
        and subtitle still have the last best-known info in them.
        """
        (work, pool_std_ebooks, pool_git, pool_gut,
            edition_std_ebooks, edition_git, edition_gut, alice, bob) = self._sample_ecosystem()

        # make sure the setup is what we expect
        eq_(pool_std_ebooks.suppressed, False)
        eq_(pool_git.suppressed, False)
        eq_(pool_gut.suppressed, False)

        # sanity check - we like standard ebooks and it got determined to be the best
        eq_(work.presentation_edition, pool_std_ebooks.presentation_edition)
        eq_(work.presentation_edition, edition_std_ebooks)

        # editions know who's the presentation edition
        eq_(edition_std_ebooks.work, work)
        eq_(edition_git.work, None)
        eq_(edition_gut.work, None)

        # The title of the Work is the title of its presentation edition.
        eq_("The Standard Ebooks Title", work.title)
        eq_("The Standard Ebooks Subtitle", work.subtitle)

        # The author of the Work is the author of its presentation edition.
        eq_("Alice Adder", work.author)
        eq_("Adder, Alice", work.sort_author)

        # now suppress all of the license pools
        pool_std_ebooks.suppressed = True
        pool_git.suppressed = True
        pool_gut.suppressed = True

        # and let work know
        work.calculate_presentation()

        # standard ebooks was last viable pool, and it stayed as work's choice
        eq_(work.presentation_edition, pool_std_ebooks.presentation_edition)
        eq_(work.presentation_edition, edition_std_ebooks)

        # editions know who's the presentation edition
        eq_(edition_std_ebooks.work, work)
        eq_(edition_git.work, None)
        eq_(edition_gut.work, None)

        # The title of the Work is still the title of its last viable presentation edition.
        eq_("The Standard Ebooks Title", work.title)
        eq_("The Standard Ebooks Subtitle", work.subtitle)

        # The author of the Work is still the author of its last viable presentation edition.
        eq_("Alice Adder", work.author)
        eq_("Adder, Alice", work.sort_author)

    def test_work_updates_info_on_pool_suppressed(self):
        """ If the provider of the work's presentation edition gets suppressed,
        the work will choose another child license pool's presentation edition as
        its presentation edition.
        """
        (work, pool_std_ebooks, pool_git, pool_gut,
            edition_std_ebooks, edition_git, edition_gut, alice, bob) = self._sample_ecosystem()

        # make sure the setup is what we expect
        eq_(pool_std_ebooks.suppressed, False)
        eq_(pool_git.suppressed, False)
        eq_(pool_gut.suppressed, False)

        # sanity check - we like standard ebooks and it got determined to be the best
        eq_(work.presentation_edition, pool_std_ebooks.presentation_edition)
        eq_(work.presentation_edition, edition_std_ebooks)

        # editions know who's the presentation edition
        eq_(edition_std_ebooks.work, work)
        eq_(edition_git.work, None)
        eq_(edition_gut.work, None)

        # The title of the Work is the title of its presentation edition.
        eq_("The Standard Ebooks Title", work.title)
        eq_("The Standard Ebooks Subtitle", work.subtitle)

        # The author of the Work is the author of its presentation edition.
        eq_("Alice Adder", work.author)
        eq_("Adder, Alice", work.sort_author)

        # now suppress the primary license pool
        pool_std_ebooks.suppressed = True

        # and let work know
        work.calculate_presentation()

        # gitenberg is next best and it got determined to be the best
        eq_(work.presentation_edition, pool_git.presentation_edition)
        eq_(work.presentation_edition, edition_git)

        # editions know who's the presentation edition
        eq_(edition_std_ebooks.work, None)
        eq_(edition_git.work, work)
        eq_(edition_gut.work, None)

        # The title of the Work is still the title of its last viable presentation edition.
        eq_("The GItenberg Title", work.title)
        eq_("The GItenberg Subtitle", work.subtitle)

        # The author of the Work is still the author of its last viable presentation edition.
        eq_("Alice Adder, Bob Bitshifter", work.author)
        eq_("Adder, Alice ; Bitshifter, Bob", work.sort_author)

    def test_different_language_means_different_work(self):
        """There are two open-access LicensePools for the same book in
        different languages. The author and title information is the
        same, so the books have the same permanent work ID, but since
        they are in different languages they become separate works.
        """
        title = 'Siddhartha'
        author = ['Herman Hesse']
        edition1, lp1 = self._edition(
            title=title, authors=author, language='eng', with_license_pool=True,
            with_open_access_download=True
        )
        w1 = lp1.calculate_work()
        edition2, lp2 = self._edition(
            title=title, authors=author, language='ger', with_license_pool=True,
            with_open_access_download=True
        )
        w2 = lp2.calculate_work()
        for l in (lp1, lp2):
            eq_(False, l.superceded)
        assert w1 != w2

    def test_reject_covers(self):
        edition, lp = self._edition(with_open_access_download=True)

        # Create a cover and thumbnail for the edition.
        base_path = os.path.split(__file__)[0]
        sample_cover_path = base_path + '/files/covers/test-book-cover.png'
        cover_href = 'http://cover.png'
        cover_link = lp.add_link(
            Hyperlink.IMAGE, cover_href, lp.data_source,
            media_type=Representation.PNG_MEDIA_TYPE,
            content=open(sample_cover_path).read()
        )[0]

        thumbnail_href = 'http://thumbnail.png'
        thumbnail_rep = self._representation(
            url=thumbnail_href,
            media_type=Representation.PNG_MEDIA_TYPE,
            content=open(sample_cover_path).read(),
            mirrored=True
        )[0]

        cover_rep = cover_link.resource.representation
        cover_rep.mirror_url = cover_href
        cover_rep.mirrored_at = datetime.datetime.utcnow()
        cover_rep.thumbnails.append(thumbnail_rep)

        edition.set_cover(cover_link.resource)
        full_url = cover_link.resource.url
        thumbnail_url = thumbnail_rep.mirror_url

        # A Work created from this edition has cover details.
        work = self._work(presentation_edition=edition)
        assert work.cover_full_url and work.cover_thumbnail_url

        # A couple helper methods to make these tests more readable.
        def has_no_cover(work_or_edition):
            """Determines whether a Work or an Edition has a cover."""
            eq_(None, work_or_edition.cover_full_url)
            eq_(None, work_or_edition.cover_thumbnail_url)
            eq_(True, cover_link.resource.voted_quality < 0)
            eq_(True, cover_link.resource.votes_for_quality > 0)

            if isinstance(work_or_edition, Work):
                # It also removes the link from the cached OPDS entries.
                for url in [full_url, thumbnail_url]:
                    assert url not in work.simple_opds_entry
                    assert url not in work.verbose_opds_entry

            return True

        def reset_cover():
            """Makes the cover visible again for the main work object
            and confirms its visibility.
            """
            r = cover_link.resource
            r.votes_for_quality = r.voted_quality = 0
            r.update_quality()
            work.calculate_presentation(search_index_client=index)
            eq_(full_url, work.cover_full_url)
            eq_(thumbnail_url, work.cover_thumbnail_url)
            for url in [full_url, thumbnail_url]:
                assert url in work.simple_opds_entry
                assert url in work.verbose_opds_entry

        # Suppressing the cover removes the cover from the work.
        index = DummyExternalSearchIndex()
        Work.reject_covers(self._db, [work], search_index_client=index)
        assert has_no_cover(work)
        reset_cover()

        # It also works with Identifiers.
        identifier = work.license_pools[0].identifier
        Work.reject_covers(self._db, [identifier], search_index_client=index)
        assert has_no_cover(work)
        reset_cover()

        # When other Works or Editions share a cover, they are also
        # updated during the suppression process.
        other_edition = self._edition()
        other_edition.set_cover(cover_link.resource)
        other_work_ed = self._edition()
        other_work_ed.set_cover(cover_link.resource)
        other_work = self._work(presentation_edition=other_work_ed)

        Work.reject_covers(self._db, [work], search_index_client=index)
        assert has_no_cover(other_edition)
        assert has_no_cover(other_work)

    def test_missing_coverage_from(self):
        operation = 'the_operation'

        # Here's a work with a coverage record.
        work = self._work(with_license_pool=True)

        # It needs coverage.
        eq_([work], Work.missing_coverage_from(self._db, operation).all())

        # Let's give it coverage.
        record = self._work_coverage_record(work, operation)

        # It no longer needs coverage!
        eq_([], Work.missing_coverage_from(self._db, operation).all())

        # But if we disqualify coverage records created before a
        # certain time, it might need coverage again.
        cutoff = record.timestamp + datetime.timedelta(seconds=1)

        eq_(
            [work], Work.missing_coverage_from(
                self._db, operation, count_as_missing_before=cutoff
            ).all()
        )

    def test_top_genre(self):
        work = self._work()
        identifier = work.presentation_edition.primary_identifier
        genres = self._db.query(Genre).all()
        source = DataSource.lookup(self._db, DataSource.AXIS_360)

        # returns None when work has no genres
        eq_(None, work.top_genre())

        # returns only genre
        wg1, is_new = get_one_or_create(
            self._db, WorkGenre, work=work, genre=genres[0], affinity=1
        )
        eq_(genres[0].name, work.top_genre())

        # returns top genre
        wg1.affinity = 0.2
        wg2, is_new = get_one_or_create(
            self._db, WorkGenre, work=work, genre=genres[1], affinity=0.8
        )
        eq_(genres[1].name, work.top_genre())

    def test_to_search_document(self):
        # Set up an edition and work.
        edition, pool = self._edition(authors=[self._str, self._str], with_license_pool=True)
        work = self._work(presentation_edition=edition)

        # Create a second Collection that has a different LicensePool
        # for the same Work.
        collection1 = self._default_collection
        collection2 = self._collection()
        self._default_library.collections.append(collection2)
        pool2 = self._licensepool(edition=edition, collection=collection2)
        pool2.work_id = work.id

        # Create a third Collection that's just hanging around, not
        # doing anything.
        collection3 = self._collection()

        # These are the edition's authors.
        [contributor1] = [c.contributor for c in edition.contributions if c.role == Contributor.PRIMARY_AUTHOR_ROLE]
        contributor1.family_name = self._str
        [contributor2] = [c.contributor for c in edition.contributions if c.role == Contributor.AUTHOR_ROLE]

        data_source = DataSource.lookup(self._db, DataSource.THREEM)

        # This identifier is strongly equivalent to the edition's.
        identifier = self._identifier()
        identifier.equivalent_to(data_source, edition.primary_identifier, 0.9)

        # This identifier is equivalent to the other identifier, but the strength
        # is too weak for it to be used.
        identifier2 = self._identifier()
        identifier.equivalent_to(data_source, identifier, 0.1)

        # Add some classifications.

        # This classification has no subject name, so the search document will use the subject identifier.
        edition.primary_identifier.classify(data_source, Subject.BISAC, "FICTION/Science Fiction/Time Travel", None, 6)

        # This one has the same subject type and identifier, so their weights will be combined.
        identifier.classify(data_source, Subject.BISAC, "FICTION/Science Fiction/Time Travel", None, 1)

        # Here's another classification with a different subject type.
        edition.primary_identifier.classify(data_source, Subject.OVERDRIVE, "Romance", None, 2)

        # This classification has a subject name, so the search document will use that instead of the identifier.
        identifier.classify(data_source, Subject.FAST, self._str, "Sea Stories", 7)

        # This classification will be left out because its subject type isn't useful for search.
        identifier.classify(data_source, Subject.DDC, self._str, None)

        # This classification will be left out because its identifier isn't sufficiently equivalent to the edition's.
        identifier2.classify(data_source, Subject.FAST, self._str, None)

        # Add some genres.
        genre1, ignore = Genre.lookup(self._db, "Science Fiction")
        genre2, ignore = Genre.lookup(self._db, "Romance")
        work.genres = [genre1, genre2]
        work.work_genres[0].affinity = 1

        # Add the other fields used in the search document.
        work.target_age = NumericRange(7, 8, '[]')
        edition.subtitle = self._str
        edition.series = self._str
        edition.publisher = self._str
        edition.imprint = self._str
        work.fiction = False
        work.audience = Classifier.AUDIENCE_YOUNG_ADULT
        work.summary_text = self._str
        work.rating = 5
        work.popularity = 4

        # Make sure all of this will show up in a database query.
        self._db.flush()


        search_doc = work.to_search_document()
        eq_(work.id, search_doc['_id'])
        eq_(work.title, search_doc['title'])
        eq_(edition.subtitle, search_doc['subtitle'])
        eq_(edition.series, search_doc['series'])
        eq_(edition.language, search_doc['language'])
        eq_(work.sort_title, search_doc['sort_title'])
        eq_(work.author, search_doc['author'])
        eq_(work.sort_author, search_doc['sort_author'])
        eq_(edition.medium, search_doc['medium'])
        eq_(edition.publisher, search_doc['publisher'])
        eq_(edition.imprint, search_doc['imprint'])
        eq_(edition.permanent_work_id, search_doc['permanent_work_id'])
        eq_("Nonfiction", search_doc['fiction'])
        eq_("YoungAdult", search_doc['audience'])
        eq_(work.summary_text, search_doc['summary'])
        eq_(work.quality, search_doc['quality'])
        eq_(work.rating, search_doc['rating'])
        eq_(work.popularity, search_doc['popularity'])

        # Each collection in which the Work is found is listed in
        # the 'collections' section.
        collections = search_doc['collections']
        eq_(2, len(collections))
        for collection in self._default_library.collections:
            assert dict(collection_id=collection.id) in collections

        contributors = search_doc['contributors']
        eq_(2, len(contributors))
        [contributor1_doc] = [c for c in contributors if c['sort_name'] == contributor1.sort_name]
        [contributor2_doc] = [c for c in contributors if c['sort_name'] == contributor2.sort_name]
        eq_(contributor1.family_name, contributor1_doc['family_name'])
        eq_(None, contributor2_doc['family_name'])
        eq_(Contributor.PRIMARY_AUTHOR_ROLE, contributor1_doc['role'])
        eq_(Contributor.AUTHOR_ROLE, contributor2_doc['role'])

        classifications = search_doc['classifications']
        eq_(3, len(classifications))
        [classification1_doc] = [c for c in classifications if c['scheme'] == Subject.uri_lookup[Subject.BISAC]]
        [classification2_doc] = [c for c in classifications if c['scheme'] == Subject.uri_lookup[Subject.OVERDRIVE]]
        [classification3_doc] = [c for c in classifications if c['scheme'] == Subject.uri_lookup[Subject.FAST]]
        eq_("FICTION Science Fiction Time Travel", classification1_doc['term'])
        eq_(float(6 + 1)/(6 + 1 + 2 + 7), classification1_doc['weight'])
        eq_("Romance", classification2_doc['term'])
        eq_(float(2)/(6 + 1 + 2 + 7), classification2_doc['weight'])
        eq_("Sea Stories", classification3_doc['term'])
        eq_(float(7)/(6 + 1 + 2 + 7), classification3_doc['weight'])

        genres = search_doc['genres']
        eq_(2, len(genres))
        [genre1_doc] = [g for g in genres if g['name'] == genre1.name]
        [genre2_doc] = [g for g in genres if g['name'] == genre2.name]
        eq_(Subject.SIMPLIFIED_GENRE, genre1_doc['scheme'])
        eq_(genre1.id, genre1_doc['term'])
        eq_(1, genre1_doc['weight'])
        eq_(Subject.SIMPLIFIED_GENRE, genre2_doc['scheme'])
        eq_(genre2.id, genre2_doc['term'])
        eq_(0, genre2_doc['weight'])

        target_age_doc = search_doc['target_age']
        eq_(work.target_age.lower, target_age_doc['lower'])
        eq_(work.target_age.upper, target_age_doc['upper'])

        # Each collection in which the Work is found is listed in
        # the 'collections' section.
        collections = search_doc['collections']
        eq_(2, len(collections))
        for collection in self._default_library.collections:
            assert dict(collection_id=collection.id) in collections

        # If the book stops being available through a collection
        # (because its LicensePool loses all its licenses or stops
        # being open access), that collection will not be listed
        # in the search document.
        [pool] = collection1.licensepools
        pool.licenses_owned = 0
        self._db.commit()
        search_doc = work.to_search_document()
        eq_([dict(collection_id=collection2.id)], search_doc['collections'])

        # If the book becomes available again, the collection will
        # start showing up again.
        pool.open_access = True
        self._db.commit()
        search_doc = work.to_search_document()
        eq_(2, len(search_doc['collections']))

    def test_target_age_string(self):
        work = self._work()
        work.target_age = NumericRange(7, 8, '[]')
        eq_("7-8", work.target_age_string)

        work.target_age = NumericRange(0, 8, '[]')
        eq_("0-8", work.target_age_string)

        work.target_age = NumericRange(8, None, '[]')
        eq_("8", work.target_age_string)

        work.target_age = NumericRange(None, 8, '[]')
        eq_("8", work.target_age_string)

        work.target_age = NumericRange(7, 8, '[)')
        eq_("7", work.target_age_string)

        work.target_age = NumericRange(0, 8, '[)')
        eq_("0-7", work.target_age_string)

        work.target_age = NumericRange(7, 8, '(]')
        eq_("8", work.target_age_string)

        work.target_age = NumericRange(0, 8, '(]')
        eq_("1-8", work.target_age_string)

        work.target_age = NumericRange(7, 9, '()')
        eq_("8", work.target_age_string)

        work.target_age = NumericRange(0, 8, '()')
        eq_("1-7", work.target_age_string)

        work.target_age = NumericRange(None, None, '()')
        eq_("", work.target_age_string)

        work.target_age = None
        eq_("", work.target_age_string)


    def test_reindex_on_availability_change(self):
        """A change in a LicensePool's availability creates a
        WorkCoverageRecord indicating that the work needs to be
        re-indexed.
        """
        work = self._work(with_open_access_download=True)
        [pool] = work.license_pools
        def find_record(work):
            """Find the Work's 'update search index operation'
            WorkCoverageRecord.
            """
            records = [
                x for x in work.coverage_records
                if x.operation.startswith(
                        WorkCoverageRecord.UPDATE_SEARCH_INDEX_OPERATION
                )
            ]
            if records:
                return records[0]
            return None
        registered = WorkCoverageRecord.REGISTERED
        success = WorkCoverageRecord.SUCCESS

        # The work starts off with no relevant WorkCoverageRecord.
        eq_(None, find_record(work))

        # If it stops being open-access, it needs to be reindexed.
        pool.open_access = False
        record = find_record(work)
        eq_(registered, record.status)

        # If its licenses_owned goes from zero to nonzero, it needs to
        # be reindexed.
        record.status = success
        pool.licenses_owned = 10
        pool.licenses_available = 10
        eq_(registered, record.status)

        # If its licenses_owned changes, but not to zero, nothing happens.
        record.status = success
        pool.licenses_owned = 1
        eq_(success, record.status)

        # If its licenses_available changes, nothing happens
        pool.licenses_available = 0
        eq_(success, record.status)

        # If its licenses_owned goes from nonzero to zero, it needs to
        # be reindexed.
        pool.licenses_owned = 0
        eq_(registered, record.status)

        # If it becomes open-access again, it needs to be reindexed.
        record.status = success
        pool.open_access = True
        eq_(registered, record.status)

        # If its collection changes (which shouldn't happen), it needs
        # to be reindexed.
        record.status = success
        collection2 = self._collection()
        pool.collection_id = collection2.id
        eq_(registered, record.status)

        # If a LicensePool is deleted (which also shouldn't happen),
        # its former Work needs to be reindexed.
        record.status = success
        self._db.delete(pool)
        work = self._db.query(Work).one()
        record = find_record(work)
        eq_(registered, record.status)


    def test_update_external_index(self):
        """Test the deprecated update_external_index method."""
        work = self._work()
        work.presentation_ready = True
        records = [
            x for x in work.coverage_records
            if x.operation==WorkCoverageRecord.UPDATE_SEARCH_INDEX_OPERATION
        ]
        index = DummyExternalSearchIndex()
        work.update_external_index(index)

        # A WorkCoverageRecord was created to register the work that
        # needs to be done.
        [record] = [
            x for x in work.coverage_records
            if x.operation==WorkCoverageRecord.UPDATE_SEARCH_INDEX_OPERATION
        ]
        eq_(WorkCoverageRecord.REGISTERED, record.status)

        # The work was not added to the search index -- that happens
        # later, when the WorkCoverageRecord is processed.
        eq_([], index.docs.values())


    def test_for_unchecked_subjects(self):

        w1 = self._work(with_license_pool=True)
        w2 = self._work()
        identifier = w1.license_pools[0].identifier

        # Neither of these works is associated with any subjects, so
        # they're not associated with any unchecked subjects.
        qu = Work.for_unchecked_subjects(self._db)
        eq_([], qu.all())

        # These Subjects haven't been checked, so the Work associated with
        # them shows up.
        ds = DataSource.lookup(self._db, DataSource.OVERDRIVE)
        classification = identifier.classify(ds, Subject.TAG, "some tag")
        classification2 = identifier.classify(ds, Subject.TAG, "another tag")
        eq_([w1], qu.all())

        # If one of them is checked, the Work still shows up.
        classification.subject.checked = True
        eq_([w1], qu.all())

        # Only when all Subjects are checked does the work stop showing up.
        classification2.subject.checked = True
        eq_([], qu.all())

    def test_calculate_opds_entries(self):
        """Verify that calculate_opds_entries sets both simple and verbose
        entries.
        """
        work = self._work()
        work.simple_opds_entry = None
        work.verbose_opds_entry = None

        work.calculate_opds_entries(verbose=False)
        simple_entry = work.simple_opds_entry
        assert simple_entry.startswith('<entry')
        eq_(None, work.verbose_opds_entry)

        work.calculate_opds_entries(verbose=True)
        # The simple OPDS entry is the same length as before.
        # It's not necessarily _exactly_ the same because the
        # <updated> timestamp may be different.
        eq_(len(simple_entry), len(work.simple_opds_entry))

        # The verbose OPDS entry is longer than the simple one.
        assert work.verbose_opds_entry.startswith('<entry')
        assert len(work.verbose_opds_entry) > len(simple_entry)

class TestSessionManager(DatabaseTest):

    def test_refresh_materialized_views(self):
        work = self._work(fiction=True, with_license_pool=True,
                          genre="Science Fiction")
        romance, ignore = Genre.lookup(self._db, "Romance")
        work.genres.append(romance)
        fiction = self._lane(display_name="Fiction", fiction=True)
        nonfiction = self._lane(display_name="Nonfiction", fiction=False)

        from model import MaterializedWorkWithGenre as mwg

        # There are no items in the materialized views.
        eq_([], self._db.query(mwg).all())

        # The lane sizes are wrong.
        fiction.size = 100
        nonfiction.size = 100

        SessionManager.refresh_materialized_views(self._db)

        # The work has been added to the materialized view. (It was
        # added twice because it's filed under two genres.)
        eq_([work.id, work.id], [x.works_id for x in self._db.query(mwg)])

        # Both lanes have had .size set to the correct value.
        eq_(1, fiction.size)
        eq_(0, nonfiction.size)
