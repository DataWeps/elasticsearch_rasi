# encoding:utf-8
require_relative 'document'
class ElasticsearchRasi
  class Mention < Document
    def initialize(es, config, es_another)
      super(es,
        config.merge(
          file:      config[:mention_file],
          idx_read:  config[:idx_mention_read],
          idx_write: config[:idx_mention_write],
          alias:     config[:mention_alias],
          type:      config[:mention_type] || 'document'),
        es_another)
    end
  end
end
