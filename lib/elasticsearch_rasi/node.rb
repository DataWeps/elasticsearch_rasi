# encoding:utf-8
require_relative 'document'
module ElasticsearchRasi
  class Node < Document
    def initialize(es, es_another, config)
      super(es, es_another, config, :node)
    end
  end
end
