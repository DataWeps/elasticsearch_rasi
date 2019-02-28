require 'active_support/core_ext/hash/deep_merge'

module ElasticsearchRasi
  class Config
    KEYS         = %i[connect_another ignore_max_age].freeze
    DEFAULT_TYPE = 'document'.freeze
    DEFAULT_ANOTHER_METHODS = %i[index update bulk].freeze

    def initialize(idx, opts)
      created_opts = \
        if opts[:direct_idx]
          # main node index (fb_page, topic, article etc ...)
          ES && ES.include?(idx.to_sym) ? ES[idx.to_sym] : opts
        else
          opts = (ES[idx.to_sym] || {}).deep_merge(opts)
          raise(ArgumentError, "Missing defined index '#{idx}'") if
            !opts || opts.empty?
          create_opts(opts)
        end
      create_methods(created_opts)
    end

    def default_values!
      @connect ||= {}
      @connect_another ||= {}
      @another_methods ||= DEFAULT_ANOTHER_METHODS
    end

    def to_json
      instance_variables.each_with_object({}) do |variable, mem|
        mem[variable[1..-1].to_sym] = instance_variable_get(variable)
      end
    end

    def include?(what)
      instance_variable_defined?(:"@#{what}".to_sym)
    end

    def merge(args = {})
      args.each do |key, value|
        self.class.send(:attr_accessor, key.to_sym)
        instance_variable_set("@#{key}", value)
      end
      self
    end

    def []=(key, value)
      instance_variable_set(:"@#{key}".to_sym, value)
    end

    def [](key)
      return instance_variable_get(:"@#{key}".to_sym) if \
        instance_variable_defined?(:"@#{key}".to_sym)
      nil
    end

  private

    def create_methods(args)
      args.each do |key, value|
        self.class.send(:attr_accessor, key.to_sym)
        instance_variable_set("@#{key}", value)
      end
      KEYS.each do |key|
        next if instance_variable_defined?(:"@#{key}".to_sym)
        self.class.send(:attr_accessor, key.to_sym)
        instance_variable_set("@#{key}", nil)
      end
      default_values!
    end

    def create_opts(opts)
      opts.deep_symbolize_keys.merge(
        node_file:               opts[:file] ? opts[:file][:node] : nil,
        mention_file:            opts[:file] ? opts[:file][:mention] : nil,
        idx_node_read:           get_index(opts, :node, :read),
        idx_node_write:          get_index(opts, :node, :write),
        idx_mention_read:        get_index(opts, :mention, :read),
        idx_mention_write:       get_index(opts, :mention, :write),
        idx_node_read_client:    get_index(opts, :node, :read, :client),
        idx_mention_read_client: get_index(opts, :mention, :read, :client),
        node_type:               opts[:node_type] || DEFAULT_TYPE,
        mention_type:            opts[:mention_type] || DEFAULT_TYPE,
        node_alias:              opts[:node_alias],
        mention_alias:           opts[:mention_alias]).merge(connect: opts[:connect])
    end

    def get_index(opts, type, access, client_type = :system)
      return nil if opts.blank?
      if client_type == :client && opts.include?("#{type}_client".to_sym)
        opts["#{type}_client".to_sym]
      elsif client_type == :system
        write = ''
        base  = "#{opts[:prefix]}#{opts[:base]}"
        index = "#{base}#{opts[:"#{type}_suffix"]}"
        "#{index}#{opts[:"#{type}_#{access}"]}#{write}"
      end
    end
  end
end
