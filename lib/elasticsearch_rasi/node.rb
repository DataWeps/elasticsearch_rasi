# encoding:utf-8
require_relative 'document'
class ElasticsearchRasi
  class Node < Document
    def initialize(es, config, es_another)
      super(es, config.merge(
        idx_read:  config[:idx_node_read],
        idx_write: config[:idx_node_write],
        alias:     config[:node_alias],
        type:      config[:node_type] || 'document'), es_another)
    end
  end
end
