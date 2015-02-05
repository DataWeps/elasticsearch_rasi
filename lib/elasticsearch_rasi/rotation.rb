# encoding:utf-8
require 'elasticsearch_rasi/util'

module Rotation

  KEYS = {
    :created   => 'created',
    :read_all  => 'all',
    :read_only => 'read_only',
  }

  # rotate_type: :node / :mention
  #              direct_idx == :node
  # opts : Hash
  #   :read_age - how long to read from index
  #   :rotation_age - how long to write to index
  #   :close_age - how long to keep index before closing (default false)
  def rotation(rotate_type, opts = {})
    return false unless check_opts(rotate_type, opts)
    # rotate read index
    rotate_read_index

    # close too old index (default not closing)
    close_index

    # rotate index for writing
    rotate_index
  end

  private

  def check_opts(rotate_type, opts)
    # YAML supports only strings
    opts        = Util::hash_str_to_hashes opts
    rotate_type = rotate_type.to_sym

    raise ArgumentError.new("Missing index in config for '#{rotate_type}'") if
      (rotate_type == :node && @idx_node_read.nil?) ||
      (rotate_type == :mention && @idx_mention_read.nil?)
      (rotate_type == :direct && @idx.nil?)
    @rotate_type, @opts_rotation = rotate_type, {}

    # check obligatory opts
    [:read_age, :rotation_age, :close_age].each { |key|
      raise ArgumentError.new("Missing rotation opts '#{key}'") unless
        opts.include?(key)
    }
    # compute time
    [:read_age, :rotation_age].each { |key|
      @opts_rotation[key] = Util::parse_date_offset(opts[key])
    }
    @opts_rotation[:close_age] = [false, nil, 'false'].include?(opts[:close_age]) ?
      false : Util::parse_date_offset(opts[:close_age])
    true
  end

  def close_index
    return true unless @opts_rotation[:close_age]

    # find index(es) removed from read, with _read_only parameter
    indexes  = get_indexes("#{recognize_index(:read)}_#{KEYS[:read_only]}")
    return true unless indexes
    indexes.each { |index_real, aliases|
      # just for sure skip current write index - contains :current sufix
      #   -- just cannot happened !
      next unless aliases['aliases'].select { |s|
        s.match(/_#{Regexp.escape(recognize_index(:write))}/)
      }.empty?
      next unless too_old?(
        parse_index_date(aliases, KEYS[:created]),
        @opts_rotation[:close_age]
      )
      # remove :read_only suffix and close index
      if request_elastic(:delete,
        "/#{index_real}/_alias/#{recognize_index(:read)}_#{KEYS[:read_only]}", {}
      )

        request_elastic(:post, "/#{index_real}/_close", {}) if too_old?(
          parse_index_date(aliases, KEYS[:created]),
          @opts_rotation[:close_age]
        )
      end
    }
  end


  # in case of too old index for read, remove read alias from index
  # args:
  #   index_alias - main alias name of index
  #   read_age    - read age of index
  def rotate_read_index
    indexes  = get_indexes(recognize_index(:read))
    return false unless indexes && indexes.count > 1
    indexes.each { |index_real, aliases|
      # skip current write index - contains :current sufix
      next unless aliases['aliases'].select { |s|
        s.match(/_#{Regexp.escape(recognize_index(:write))}/)
      }.empty?

      next unless too_old?(
        parse_index_date(aliases, KEYS[:created]),
        @opts_rotation[:read_age]
      )

      # remove :read alias (name of alias depends on configuration on current app)
      request_elastic(
        :delete, "/#{index_real}/_alias/#{recognize_index(:read)}", {}
      )
    }
  end

  # create new write index
  # args:
  #   index_alias - main alias name of index
  #   rotation_age - age of write index
  def rotate_index
    return true if @opts_rotation[:rotation_age].nil?
    # rotation in progress ?
    return false unless result =
      get_indexes("#{recognize_index(:write)}")
    if result.count > 1
      GLogg.l_f{"#{self.class}.perform: Two current indexes for" +
        " '#{recognize_index(:write)}' : #{result.keys}"}
      return false
    end

    # we need to recognize real index name
    index_real = result.keys.first
    date       = parse_index_date(result[index_real], KEYS[:created])
    return false unless too_old?(date, @opts_rotation[:rotation_age])

    settings   = request_elastic :get, "/#{index_real}/_settings"
    mapping    = request_elastic :get, "/#{index_real}/_mapping"

    # build settings
    build_settings  = {}
    settings[index_real]['settings'].each { |key_s, value_s|
      h = build_settings
      key_s.split('.').each { |under_key|
        if under_key == key_s.split('.').last
          h[under_key] = value_s
          break
        else
          h.merge!({under_key => h[under_key] || {}})
          h = h[under_key]
        end
      }
    }

    # build mapping
    mapping    = mapping[index_real]

    # hack for ES in version 1.*
    mapping    = mapping['mappings'] if
      mapping.include?('mappings') && mapping.keys.count == 1
    type       = mapping.keys.first
    date_index = Time.now.strftime('%Y_%m_%d')

    # new name based on configuration on server
    new_base_prefix = "#{@idx_opts[:base]}#{@idx_opts[:"#{@rotate_type}_suffix"]}"
    # new index real name
    new_base_index  = "#{new_base_prefix}_#{Time.now.strftime('%Y%m%d%H%M')}"

    build_settings['index'].delete 'version' if
      build_settings && build_settings['index'] &&
      build_settings['index']['version']

    # create new index with settngs
    return false unless request_elastic(
      :post,
      new_base_index,
      Oj.dump({settings: build_settings})
    )

    # put mapping to index
    request_elastic(:put,
      "#{new_base_index}/#{type}/_mapping", Oj.dump(mapping)
    ) if type && !type.empty?

    # read from index
    request_elastic(:put,
      "#{new_base_index}/_alias/#{recognize_index(:read)}", {})

    # alias for read from all indexes
    request_elastic(:put,
      "#{new_base_index}/_alias/#{new_base_prefix}_#{KEYS[:read_all]}", {})

    # create alias for writing
    request_elastic(:put,
      "#{new_base_index}/_alias/#{recognize_index(:write)}", {})

    # disable old index for writing
    request_elastic(:delete,  "#{index_real}/_alias/" +
      "#{recognize_index(:write)}", {})

    # add created information
    request_elastic(:put,
      "#{new_base_index}/_alias/#{new_base_prefix}" +
        "_#{KEYS[:created]}_#{date_index}", {}
    )

    # add information about read only to old index
    request_elastic(:put, "#{index_real}/_alias/" +
      "#{new_base_prefix}_#{KEYS[:read_only]}", {})

  end

  def recognize_index access
    return @idx if @rotate_type == :direct
    value = eval "@idx_#{@rotate_type}_#{access.to_s}"
    return nil unless value
    value
  end

  # get index with current alias
  def get_indexes(index_alias)
    result = request_elastic :get, "#{index_alias}/_alias/*"
    return false unless result # already logged
    if result['error']
      GLogg.l_f{ "#{self.class}.perform: Unknown index '#{index_alias}'" }
      return nil
    end
    result
  end

  # get date from index alias
  def parse_index_date(key, sufix)
    date = key['aliases'].select { |a| a.match(/#{sufix}_(.+)/) }
    if date.count == 0
      GLogg.l_f{"#{self.class}.perform: Unknow index alias for recognize age." +
        " Didn't find sufix '#{sufix}' for " +
        "'#{key['aliases']}'"}
      return nil
    end
    date = date.keys.first =~ /#{sufix}_(.+)/i ? $1.gsub('_', '-') : nil
  end

  # equal index date AND age date
  def too_old?(date, age)
    return false unless age
    !!(date && (Time.parse(date).to_i < age.to_i))
  end

end # Rotation
