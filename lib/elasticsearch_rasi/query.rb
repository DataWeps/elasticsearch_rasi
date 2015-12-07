# encoding:utf-8

class ElasticsearchRasi
  module Query
    def get_docs_query(query, size = ElasticsearchRasi::SLICES)
      {
        "size"  => size,
        "query" => {
          "filtered" => {
            "filter" => query,
            "_cache" => false
          }
        }
      }
    end

    def get_docs_query_ids(query, size = ElasticsearchRasi::SLICES)
      {
        "size"   => size,
        "fields" => [],
        "query" => {
          "filtered" => {
            "filter" => query,
            "_cache" => false
          }
        }
      }
    end

    def get_bool_query(query)
      {
        "query" => {
          "filtered" => {
            "filter" => {
              "bool" => query
            },
            "_cache" => false
          }
        }
      }
    end

    def get_count_query(query)
      {
        "size" => 1,
        "query" => {
          "filtered" => {
            "filter" => {
              "bool" => query
            },
            "_cache" => false
          }
        }
      }
    end
  end
end
