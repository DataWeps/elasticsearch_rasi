# encoding:utf-8

module Mention

  # alias method for getting mentions
  def get_mentions(ids, idx = @idx_mention_read, type = 'document')
    get_docs(ids, idx, type)
  end

  # alias method for saving mentions
  def save_mentions(mentions, idx = @idx_mention_write, type = 'document')
    save_docs(mentions, idx, type)
  end

end