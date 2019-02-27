module ElasticsearchRasi
  class Queries
    QUERIES = {
      mget_query: %({
        "docs": [
        ]
      }),
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
      }) }.freeze
    class << self
      def prepare_query(what, query, size = nil)
        temp_query = JsonHelper.load(QUERIES[what])
        temp_query['size'] = size if size
        if what == :mget_query
          temp_query['docs'] = query.map { |id| { '_id' => id } }
        elsif query
          temp_query['query']['bool']['filter'] = query
        end
        temp_query
      end
    end
  end
end
