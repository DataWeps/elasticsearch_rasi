# encoding:utf-8
require_relative 'document'
module ElasticsearchRasi
  class Mention < Document
    def initialize(es, config, es_another)
      @rasi_type = :mention
      super(es, es_another, config, :mention)
    end
  end
end
