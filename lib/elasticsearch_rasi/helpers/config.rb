require 'utils/refines/time_index_name'

require 'active_support/core_ext/hash/deep_merge'

module ElasticsearchRasi
  class Config
    using TimeIndexName
    KEYS         = %i[connect_another ignore_max_age connect_attempts].freeze
    DEFAULT_TYPE = 'document'.freeze
    DEFAULT_ANOTHER_METHODS = %i[index update bulk].freeze
    CONNECT_ATTEMPTS        = 3
    CONNECT_SLEEP           = 1
    DEFAULT_MAX_AGE         = 6

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
      compute_dates!
    end

    def default_values!
      self[:connect] ||= {}
      self[:connect_another]  ||= {}
      self[:connect_attempts] ||= CONNECT_ATTEMPTS
      self[:connect_sleep]    ||= CONNECT_SLEEP
      self[:another_methods]  ||= DEFAULT_ANOTHER_METHODS
      (self[:another_methods] || []).map!(&:to_sym)
      self[:read_date_months] ||= []
      self[:type]             ||= 'document'
      self[:max_age]            = nil
      self[:languages_write]  ||= false
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
      args.each { |key, value| self[key] = value }
      self
    end

    def []=(key, value)
      self.class.send(:attr_accessor, key.to_sym)
      instance_variable_set(:"@#{key}".to_sym, value)
    end

    def [](key)
      return instance_variable_get(:"@#{key}".to_sym) if \
        instance_variable_defined?(:"@#{key}".to_sym)
      nil
    end

    def compute_dates!
      return unless self[:rasi_type]
      self[:write_date] = compute_write_date?
      self[:read_date]  = compute_read_date?
      self[:language_index] = compute_lang_index?
    end

  private

    def compute_read_date?
      if self[concat_rasi_type(suffix: '_read_date')].present?
        from_month = Time.now.months_ago(
          self[concat_rasi_type(suffix: '_max_age')] || DEFAULT_MAX_AGE).beginning_of_month
        this_month = Time.now.end_of_month
        self[:read_date_months] ||= []
        loop do
          break if from_month > this_month
          new_month = create_new_month(from_month)
          self[:read_date_months] << new_month unless \
            self[:read_date_months].include?(new_month)

          from_month = from_month.months_since(1)
        end
        true
      else
        false
      end
    end

    def create_new_month(from_month)
      "#{self[concat_rasi_type(suffix: '_read_date_base')] ||
       self[concat_rasi_type(prefix: 'idx', suffix: '_read')]}" \
      "_#{from_month.index_name_date}"
    end

    def compute_lang_index?
      self[concat_rasi_type(suffix: '_language_index')].present?
    end

    def compute_write_date?
      if self[concat_rasi_type(suffix: '_write_date')].present?
        recognize_max_age!
        true
      else
        false
      end
    end

    def concat_rasi_type(suffix: '', prefix: '')
      "#{prefix}#{@rasi_type}#{suffix}".to_sym
    end

    # for data moving between elastics, we need to keep recognizing index based on published_at
    #   but at the same time, we want to save all the mentions into the database
    #   and ignore max_age
    # otherwise
    #   set @max_age
    def recognize_max_age!
      self[:max_age] = nil
      return if self[:ignore_max_age]
      self[:max_age] = Time.now.months_ago(
        self[concat_rasi_type(suffix: '_max_age')] || DEFAULT_MAX_AGE) \
                           .beginning_of_month.to_i
    end

    def create_methods(args)
      args.each do |key, value|
        self[key] = value
      end
      KEYS.each do |key|
        next if instance_variable_defined?(:"@#{key}".to_sym)
        self[key] = nil
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
