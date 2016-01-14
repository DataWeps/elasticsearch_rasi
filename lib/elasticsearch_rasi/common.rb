# encoding: utf-8

class ElasticsearchRasi
  module Common
    # prepare array indexes,lengths in the manner of slices
    # e.g. for a.length=25 and cnt=10 return [[0,10],[10,10],[20,5]]
    def array_slice_indexes(ids, cnt = SLICES)
      rslt = []
      rslt << ids.shift(cnt) until ids.empty?
      rslt
    end

    # translate results from ES to {id => doc}
    def parse_response(response, docs = {})
      response['hits']['hits'].each { |doc| docs[doc['_id']] = doc['_source'] }
    end

    def create_bulk(slice, idx, type = 'document', method = :index)
      bulk = slice.map do |doc|
        id_save = doc.delete("_id") || next
        doc = { :doc => doc } if method == :update
        {
          method => {
            _index: idx,
            _id:    id_save,
            _type:  type,
            data:   doc
          }
        }
      end
      bulk.compact
    end
  end
end
