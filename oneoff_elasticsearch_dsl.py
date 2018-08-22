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

class ExternalSearchIndex(object):

    def __init__(self, url, index, medium='Book', language='eng',
                 fiction=None, audience='Children'):
        self.client = Elasticsearch(url, use_ssl=True, timeout=20, maxsize=25)
        self.qu = Search(using=self.client)
        self.media = [medium.lower()]
        self.language = language.lower()
        self.fiction = fiction
        self.audiences = [audience.lower()]

    def search(self, query_string):
        query = self.build_query(query_string)
        filter = self.filter()
        if filter:
            query = Q("filtered", query=query, filter=self.filter())
        print pprint.pprint(query.to_dict())
        results = self.qu.query(query)
        return results

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

        #"tennis",
        
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

    def _boost(self, boost, queries):
        """Boost a query by a certain amount relative to its neighbors in a
        dis_max query.
        """
        if not isinstance(queries, list):
            queries = [queries]
        return Q("bool", boost=boost, minimum_should_match=1, should=queries)

    def simple_query_string_query(self, query_string):
        q = Q("simple_query_string", query=query_string, fields=self.SIMPLE_QUERY_STRING_FIELDS)
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
            type="best_fields", fuzziness="AUTO", prefix_length=2,
            query=query_string, max_expansions=2
        )
        return fuzzy

    def _match_phrase(self, field, query_string):
        """A clause that matches the query string against a specific field in the search document."""
        return Q("match_phrase", **{field: query_string})

    def _exact_field_match(self, field, query_string):
        """A clause for use in a dis_max query which gives a great
        score if a field matches exactly.
        """
        return self._match_phrase(field, query_string)

    def minimal_stemming_search(self, query_string, fields):
        return [self._match_phrase(field, query_string) for field in fields]


    def build_query(self, query_string):
        # This search query will test a number of hypotheses about what
        # the query string might 'really' mean.
        hypotheses = []
        def hypothesize(query, boost=1):
            if boost > 1:
                query = self._boost(boost, query)
            hypotheses.append(query)

        # The query string might appear in one of the standard
        # searchable fields.
        simple = self.simple_query_string_query(query_string)
        hypothesize(simple)

        # The query string might be a near-exact match against title,
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
            self._exact_field_match("title.standard", query_string), 200
        )
        hypothesize(
            self._exact_field_match("author.standard", query_string), 200
        )

        # The query string might be a fuzzy match against one of the
        # standard searchable fields.
        fuzzy = self.fuzzy_string_query(query_string)
        if fuzzy:
            hypothesize(fuzzy)

        # The query string might contain some filtering information
        # (e.g. a genre name or target age), with the remainder being
        # the "real" query string.
        # 
        # (That description makes it sound like _subquery_with_filter
        # here should call build_query recursively, but there seem
        # to be complications.)
        #
        # if allow_subquery:
        #     poss.append(self._subquery_with_filter(query_string))
        

        # For a given book, whichever one of these hypotheses gives
        # the highest score should be used.
        qu = Q("dis_max", queries=hypotheses)
        return qu

    def filter(self):
        return None
        value = None
        f = F('term', language=self.language)
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

index = "circulation-works-current"
search = ExternalSearchIndex(url, index, fiction=True)
import sys
if len(sys.argv) > 1:
    query = sys.argv[1]
else:
    query = 'web development software (non-microsoft)'
query_obj = search.search(query)

max = 10
a = 0
for result in query_obj.scan():
    print '"%s" (%s) by %s %s %s' % (result['title'], result['subtitle'], result['author'], result['series'], query.lower() in (result.summary or '').lower()) 
    #if not query.lower() in (result['summary'] or '').lower():
    #    print result['summary']
    #print result['title'], result['author'], result['medium'], result['series']
    a += 1
    if a > max:
        break
