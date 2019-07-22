# encoding:utf-8
require_relative 'document'
module ElasticsearchRasi
  class Mention < Document
    def initialize(es, es_another, config)
      super(es, es_another, config, :mention)
    end
  end
end
