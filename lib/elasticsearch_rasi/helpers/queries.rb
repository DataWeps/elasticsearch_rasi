module ElasticsearchRasi
  class Queries
    QUERIES = {
      docs_query: %({
        "size": 0,
        "query": {
          "bool": {
            "filter": {},
            "_cache": false
          }
        }
      }),
      docs_query_ids: %({
          "size": 0,
          "_source": [],
          "query": {
            "bool": {
              "filter": {},
              "_cache": false
            }
          }
        }),
      bool_query: %({
        "query": {
          "bool": {
            "filter": {
              "bool": {}
            },
            "_cache": false
          }
        }
      }),
      count_query: %(      {
        "size": 0,
        "query": {
          "bool": {
            "filter": {
            },
            "_cache": false
          }
        }
      })
    }
    class << self
      def prepare_query(what, query, size = nil)
        temp_query = JsonHelper.load(QUERIES[what])
        temp_query['size'] = size if size
        temp_query['query']['bool']['filter'] = query if query
        puts temp_query
        temp_query
      end
    end
  end
end
