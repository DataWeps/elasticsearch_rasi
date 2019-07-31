require 'utils/json_helper'

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
      filter_query: %({
        "query": {
          "bool": {
            "filter": {
            }
          }
        }
      }),
      bool_query: %({
        "query": {
          "bool": {
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
      def prepare_query(what, query, size = nil, idx = nil)
        temp_query = JsonHelper.load(QUERIES[what])
        temp_query['size'] = size if size
        if what == :mget_query
          temp_query['docs'] = [idx].flatten.each_with_object([]) do |index, mem|
            query.each do |id|
              mem << { '_index' => index, '_id' => id }
            end
          end
        elsif what == :bool_query
          temp_query['query']['bool'] = query
        else
          temp_query['query']['bool']['filter'] = query
        end
        temp_query
      end
    end
  end
end
