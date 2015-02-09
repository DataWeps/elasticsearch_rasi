# encoding:utf-8

module Node

  # alias method for getting node document (page, user, group...)
  def get_node(ids, idx = @idx_node_read, type = 'document')
    @idx_opts[:node_alias] ?
      get_docs_by_filter(ids, idx, type) : get_docs_by_mget(ids, idx, type)
  end

  # alias method for saving node document (page, user, group...)
  def save_node(node, idx = @idx_node_write, type = 'document')
    save_docs(node, idx, type)
  end

end
