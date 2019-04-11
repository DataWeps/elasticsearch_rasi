# encoding:utf-8
require_relative 'document'
module ElasticsearchRasi
  class Node < Document
    def initialize(es, config, es_another)
      @rasi_type = :node
      super(es, config.merge(
        file:      config[:node_file],
        idx_read:  config[:idx_node_read],
        idx_write: config[:idx_node_write],
        alias:     config[:node_alias],
        rasi_type: :node,
        type:      config[:node_type] || 'document'), es_another)
    end
  end
end
