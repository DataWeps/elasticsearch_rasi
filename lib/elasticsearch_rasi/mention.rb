# encoding:utf-8

class ElasticSearchRasi

  module Mention

    # alias method for getting mentions
    def get_mentions(ids, idx = @idx_mention_read, type = 'document')
      @idx_opts[:mention_alias] ?
        get_docs_by_filter(ids, idx, type) : get_docs_by_mget(ids, idx, type)
    end

    # alias method for saving mentions
    def save_mentions(mentions, idx = @idx_mention_write, type = 'document')
      save_docs(mentions, idx, type)
    end

  end

end