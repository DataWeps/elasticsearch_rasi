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
      response['hits']['hits'].each_with_object(docs) { |doc, mem| mem[doc['_id']] = doc['_source'] }
    end

    def prepare_index(index, doc)
      return index if doc.blank? || !@write_date || doc['published_at'].blank?
      parsed_published_at = Time.parse(doc['published_at'])
      return nil if parsed_published_at < @max_age
      "#{index}_#{parsed_published_at.strftime('%Y%d')}"
    end

    def create_bulk(slice, idx, method = :index, type = 'document')
      slice.map do |doc|
        id_save = doc.delete('_id') || next
        index = prepare_index(idx, doc) || next
        doc = { :doc => doc } if method == :update
        {
          method => {
            _index: index,
            _id:    id_save,
            _type:  type }.merge(doc.empty? ? {} : { data: doc }) }
      end.compact
    end
  end
end
