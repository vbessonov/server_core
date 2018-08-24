from pdb import set_trace
import re
import pprint
from elasticsearch import Elasticsearch
from elasticsearch_dsl import (
    Search,
    Q,
    F,
)
from classifier import (
    KeywordBasedClassifier,
    GradeLevelClassifier,
    AgeClassifier,
)

baseline = baseline = {'query': {'filtered': {'filter': {'and': [{'or': [{'terms': {'collection_id': [1,
                                                                                2,
                                                                                3,
                                                                                4,
                                                                                5]}},
                                                   {'bool': {'must_not': {'exists': {'field': 'collection_id'}}}}]},
                                           {'terms': {'language': [u'eng']}},
                                           {'terms': {'medium': ['book']}}]},
                        'query': {'dis_max': {'queries': [{'simple_query_string': {'fields': ['title^4',
                                                                                              'series^4',
                                                                                              'subtitle^3',
                                                                                              'summary^2',
                                                                                              'classifications.term^2',
                                                                                              'author^6',
                                                                                              'publisher',
                                                                                              'imprint'],
                                                                                   'query': u'mary shelley science fiction'}},
                                                          {'bool': {'boost': 100,
                                                                    'minimum_should_match': 1,
                                                                    'should': [{'match_phrase': {'title.minimal': u'mary shelley science fiction'}},
                                                                               {'match_phrase': {'author': u'mary shelley science fiction'}},
                                                                               {'match_phrase': {'series.minimal': u'mary shelley science fiction'}}]}},
                                                          {'bool': {'boost': 200,
                                                                    'minimum_should_match': 1,
                                                                    'should': [{'match_phrase': {'title.standard': u'mary shelley science fiction'}}]}},
                                                          {'bool': {'boost': 200,
                                                                    'minimum_should_match': 1,
                                                                    'should': [{'match_phrase': {'author.standard': u'mary shelley science fiction'}}]}},
                                                          {'multi_match': {'fields': ['title.minimal^4',
                                                                                      'series.minimal^4',
                                                                                      'subtitle.minimal^3',
                                                                                      'summary.minimal^2',
                                                                                      'author^4',
                                                                                      'publisher',
                                                                                      'imprint'],
                                                                           'fuzziness': 'AUTO',
                                                                           'query': u'mary shelley science fiction',
                                                                           'type': 'best_fields'}},
                                                          {'bool': {'boost': 200.0,
                                                                    'must': [{'match': {'genres.name': u'Science Fiction'}},
                                                                             {'match': {'fiction': 'Fiction'}},
                                                                             {'simple_query_string': {'fields': ['author^4',
                                                                                                                 'subtitle^3',
                                                                                                                 'summary^5',
                                                                                                                 'title^1',
                                                                                                                 'series^1'],
                                                                                                      'query': u'mary shelley '}}]}}]}}}}}


class Query(object):

    def __init__(self, searcher, medium='Book', language='eng', fiction=None, audience=None,
                 target_age=None):
        self.searcher = searcher
        if medium:
            self.media = [medium.lower()]
        else:
            self.media = None
        self.languages = [language.lower()]
        self.fiction = fiction
        if audience:
            self.audiences = [audience.lower()]
        else:
            self.audiences = None

        if target_age:
            self.target_age = target_age

    def search(self, query_string):
        # Build the query.
        query = self.build_query(query_string)

        # Add the filter, if necessary.
        filter = self.filter()
        if filter:
            query = Q("filtered", query=query, filter=self.filter())

        # Run the query.
        results = self.searcher.query(query)
        print pprint.pprint(results.to_dict())
        return results

    def build_query(self, query_string):
        # This search query will test a number of hypotheses about what
        # the query string might 'really' mean.
        hypotheses = []
        def hypothesize(query, boost=15):
            if boost > 1:
                query = self._boost(boost, query)
            hypotheses.append(query)

        # The query string might appear in one of the standard
        # searchable fields.
        simple = self.simple_query_string_query(query_string)
        hypothesize(simple)

        # The query string might be a close match against title,
        # author, or series.
        hypothesize(
            self.minimal_stemming_search(
                query_string, ['title.minimal', 'author', 'series.minimal']
            ),
            100
        )

        # The query string might be an exact match for title or
        # author. Such a match would be boosted quite a lot.
        hypothesize(
            self._match_phrase("title.standard", query_string), 200
        )
        hypothesize(
            self._match_phrase("author.standard", query_string), 200
        )

        # The query string might be a fuzzy match against one of the
        # standard searchable fields.
        fuzzy = self.fuzzy_string_query(query_string)
        if fuzzy:
            hypothesize(fuzzy, 1)

        # The query string might contain some specific field matches
        # (e.g. a genre name or target age), with the remainder being
        # the "real" query string.
        with_field_matches = self._query_with_field_matches(query_string)
        if with_field_matches:
            hypothesize(with_field_matches)

        # For a given book, whichever one of these hypotheses gives
        # the highest score should be used.
        qu = Q("dis_max", queries=hypotheses)
        return qu

    def _boost(self, boost, queries):
        """Boost a query by a certain amount relative to its neighbors in a
        dis_max query.
        """
        if not isinstance(queries, list):
            queries = [queries]
        return Q("bool", boost=float(boost), minimum_should_match=1, should=queries)

    # When we run a simple query string search, we are matching the
    # query string against these fields.
    SIMPLE_QUERY_STRING_FIELDS = [
        # These fields have been stemmed.
        'title^4',
        "series^4",
        'subtitle^3',
        'summary^2',
        "classifications.term^2",

        # These fields only use the standard analyzer and are closer to the
        # original text.
        'author^6',
        'publisher',
        'imprint'
    ]

    # When we run a fuzzy query string search, we are matching the
    # query string against these fields. It's more important that we
    # use fields that have undergone minimal stemming because the part
    # of the word that was stemmed may be the part that is misspelled
    FUZZY_QUERY_STRING_FIELDS = [
        'title.minimal^4',
        'series.minimal^4',
        "subtitle.minimal^3",
        "summary.minimal^2",
        'author^4',
        'publisher',
        'imprint'
    ]

    # These words will fuzzy-match other common words that aren't relevant,
    # so if they're present and correctly spelled we shouldn't use a
    # fuzzy query.
    FUZZY_CONFOUNDERS = [
        "baseball", "basketball", # These fuzzy match each other

        "soccer", # Fuzzy matches "saucer", "docker", "sorcery"

        "football", "softball", "software", "postwar",

        # "tennis",

        "hamlet", "harlem", "amulet", "tablet",

        "biology", "ecology", "zoology", "geology",

        "joke", "jokes" # "jake"

        "cat", "cats",
        "car", "cars",
        "war", "wars",

        "away", "stay",
    ]

    # If this regular expression matches a query, we will not run
    # a fuzzy match against that query, because it's likely to be
    # counterproductive.
    FUZZY_CIRCUIT_BREAKER = re.compile(
        r'\b(%s)\b' % "|".join(FUZZY_CONFOUNDERS), re.I
    )

    def simple_query_string_query(self, query_string, fields=None):
        fields = fields or self.SIMPLE_QUERY_STRING_FIELDS
        q = Q("simple_query_string", query=query_string, fields=fields)
        return q

    def fuzzy_string_query(self, query_string):
        # TODO: If all (or a high percentage) of the words in the
        # query string look like correctly spelled English words,
        # don't run a fuzzy query. This can potentially replace the
        # use of FUZZY_CIRCUIT_BREAKER.
        #if self.FUZZY_CIRCUIT_BREAKER.search(query_string):
        #    return None
        fuzzy = Q(
            "multi_match", fields=self.FUZZY_QUERY_STRING_FIELDS,
            type="best_fields", fuzziness="AUTO",
            query=query_string,
            prefix_length=1,
        )
        return fuzzy

    def _match_phrase(self, field, query_string):
        """A clause that matches the query string against a specific field in the search document.

        The words in the query_string must match the words in the field,
        in order. E.g. "fiction science" will not match "Science Fiction".
        """
        return Q("match_phrase", **{field: query_string})

    def _match(self, field, query_string):
        """A clause that matches the query string against a specific field in the search document.
        """
        return Q("match", minimum_should_match=3, **{field: query_string})

    def minimal_stemming_search(self, query_string, fields):
        return [self._match_phrase(field, query_string) for field in fields]

    def _match_range(self, field, operation, value):
        match = {field : {operation: value}}
        return dict(range=match)

    def make_target_age_query(self, target_age, boost=1):
        (lower, upper) = target_age[0], target_age[1]
        # There must be _some_ overlap with the provided range.
        must = [
            self._match_range("target_age.upper", "gte", lower),
            self._match_range("target_age.lower", "lte", upper)
        ]

        # Results with ranges closer to the query are better
        # e.g. for query 4-6, a result with 5-6 beats 6-7
        should = [
            self._match_range("target_age.upper", "lte", upper),
            self._match_range("target_age.lower", "gte", lower),
        ]

        return Q("bool", must=must, should=should, boost=boost)

    def _query_with_field_matches(self, query_string):
        """Deal with a query string that contains information that should be
        exactly matched against a controlled vocabulary
        (e.g. "nonfiction" or "grade 5") along with information that
        is more search-like (such as a title or author).

        The match information is pulled out of the query string
        and used to make a series of match_phrase queries. The rest of
        the information is used in a simple 
        """
        original_query_string = query_string

        def without_match(query_string, match):
            """Take the portion of a query string that matched a controlled
            vocabulary, and remove it from the query string, so it
            doesn't get reused in another part of this method.
            """
            # If the match was "children" and the query string was
            # "children's", we want to remove the "'s" as well as
            # the match. We want to remove everything up to the
            # next word boundary that's not an apostrophe or a
            # dash.
            word_boundary_pattern = r"\b%s[\w'\-]*\b"

            return re.compile(
                word_boundary_pattern % match.strip(), re.IGNORECASE
            ).sub("", query_string)

        # We start with no match queries.
        match_queries = []

        def add_match_query(query, field, query_string, matched_portion):
            """Create a match query that finds documents whose value for `field`
            matches `query`.

            Add it to `match_queries`, and remove the relevant portion
            of `query_string` so it doesn't get reused.
            """
            if not query:
                # This is not a relevant part of the query string.
                return query_string
            match_query = self._match(field, query)
            match_queries.append(match_query)
            return without_match(query_string, matched_portion)

        def add_target_age_query(query, query_string, matched_portion):
            """Create a query that finds documents whose value for `target_age`
            matches `query`.

            Add it to `match_queries`, and remove the relevant portion
            of `query_string` so it doesn't get reused.
            """
            if not query:
                # This is not a relevant part of the query string.
                return query_string
            match_query = self.make_target_age_query(query, 40)
            match_queries.append(match_query)
            return without_match(query_string, matched_portion)

        # We handle genre first so that later matches don't see genre
        # names like 'Science Fiction'.

        # Handle the 'romance' part of 'young adult romance'
        genre, genre_match = KeywordBasedClassifier.genre_match(query_string)
        if genre:
            query_string = add_match_query(
                genre.name, 'genres.name', query_string, genre_match
            )

        # Handle the 'young adult' part of 'young adult romance'
        audience, audience_match = KeywordBasedClassifier.audience_match(
            query_string
        )
        if audience:
            query_string = add_match_query(
                audience.replace(" ", ""), 'audience', query_string,
                audience_match
            )

        # Handle the 'nonfiction' part of 'asteroids nonfiction'
        fiction = None
        if re.compile(r"\bnonfiction\b", re.IGNORECASE).search(query_string):
            fiction = "Nonfiction"
        elif re.compile(r"\bfiction\b", re.IGNORECASE).search(query_string):
            fiction = "Fiction"
        query_string = add_match_query(
            fiction, 'fiction', query_string, fiction
        )

        # Handle the 'grade 5' part of 'dogs grade 5'
        age_from_grade, grade_match = GradeLevelClassifier.target_age_match(query_string)
        if age_from_grade and age_from_grade[0] == None:
            age_from_grade = None
        query_string = add_target_age_query(
            age_from_grade, query_string, grade_match
        )

        # Handle the 'age 10 and up' part of 'divorce age 10 and up'
        age, age_match = AgeClassifier.target_age_match(query_string)
        if age and age[0] == None:
            age = None
        query_string = add_target_age_query(age, query_string, age_match)

        if query_string == original_query_string:
            # We didn't find anything that indicates this is a search
            # that includes a field match component. So this method should
            # not try to modify the search document at all.
            return None

        if len(query_string.strip()) > 0:
            # Someone who searched for 'young adult romance'
            # now has an empty query string -- they matched an audience
            # and a genre, and now there's nothing else to match.
            #
            # Someone who searched for 'asteroids nonfiction'
            # still has a query string of 'asteroids'. Their query string
            # has a field match component and a query-type component.
            #
            # What is likely to be in this query-type component?
            #
            # In theory, it could be anything that would go into a
            # regular query. So would be a really cool place to
            # call build_query recursively.
            #
            # However, someone who searches by genre is probably
            # not looking for a specific book. They might be
            # looking for an author (eg, 'science fiction iain
            # banks'). But they're most likely searching for a
            # _type_ of book, which means a match against summary or
            # subject ('asteroids')  would be the most useful.
            match_rest_of_query = self.simple_query_string_query(
                query_string.strip(),
                ["author^4", "subtitle^3", "summary^5", "title^1", "series^1",
                ]
            )
            match_queries.append(match_rest_of_query)

        # If all of the match queries match, the result will have a
        # higher score than results that match the full query in one
        # of the main fields.
        return Q('bool', must=match_queries, boost=200.0)

    def filter(self):
        """Make sure that the query only finds titles that belong
        in the current lane.
        """
        value = None
        f = F('term', language=self.languages)
        if self.fiction is not None:
            if self.fiction:
                value = 'fiction'
            else:
                value = 'nonfiction'
            f = f & F('term', fiction=value)
        if self.media:
            f = f & F('term', medium=self.media)
        if self.audiences:
            f = f & F('term', audience=self.audiences)
        return f

qa = "https://search-qa-nypl-circ-ob3hfe2f7sicuhearikkuvmejm.us-east-1.es.amazonaws.com/"
# prod = "http://search-prod-simplfied-3lf56oxhbjcp63fwractcqxohi.us-east-1.es.amazonaws.com:80/"
url = qa
index = "circulation-works-current"

client = Elasticsearch(url, use_ssl=False, timeout=20, maxsize=1000)
search = Search(using=client)
query_obj = Query(search)
import sys
if len(sys.argv) > 1:
    query = sys.argv[1]
else:
    query = 'web development software (non-microsoft)'
results = query_obj.search(query)

a = 0
for result in results[0:2000]:
    good = "FFF"
    if query.lower() in result.title.lower() or query.lower() in (result.subtitle or "").lower():
        good = "AAA"
    elif query.lower() in (result.summary or "").lower():
        good = "BBB"
    elif result['genres'] and 'Computers' in [x['name'] for x in result['genres']]:
        good = "CCC"


    out = '%3d %s "%s" (%s) by %s %s %s' % (a, good, result['title'], result['subtitle'], result['author'], result['series'], query.lower() in (result.summary or '').lower())
    print out.encode("utf8")
    a += 1
    #if not query.lower() in (result['summary'] or '').lower():
    #    print result['summary']
    #print result['title'], result['author'], result['medium'], result['series']
print "%d total" % a
