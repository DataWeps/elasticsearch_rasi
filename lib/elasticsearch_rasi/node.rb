# encoding:utf-8

module Node

  # alias method for getting node document (page, user, group...)
  def get_node(id, idx = @idx_node_read, type = 'document')
    get_docs(id, idx, type)
  end

  # alias method for saving node document (page, user, group...)
  def save_node(node, idx = @idx_node_write, type = 'document')
    save_docs(node, idx, type)
  end

end
