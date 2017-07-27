# encoding: utf-8
require 'unicode_utils/downcase'
require 'digest/sha1'

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
      return nil if parsed_published_at.to_i < @max_age ||
                    parsed_published_at.to_i > (Time.now.to_i + (3 * 3600))
      "#{index}_#{parsed_published_at.strftime('%Y%m')}"
    end

    def prepare_search_author!(doc)
      author =
        if doc['author'].is_a?(Hash)
          doc['author']['name']
        else
          doc['author']
        end

      doc['search_author'] = {
        'name' => author,
        'author_hash' => compute_author_hash(author) }
    end

    def compute_author_hash(author)
      UnicodeUtils.downcase(Digest::SHA1.hexdigest(author))
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
