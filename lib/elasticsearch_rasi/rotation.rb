# encoding:utf-8
require 'elasticsearch_rasi/util'

class ElasticSearchRasi

  module Rotation

    KEYS = {
      :created   => 'created',
      :read_all  => 'all',
      :read_from => 'read_from',
    }

    # rotate_type: :node / :mention
    #              direct_idx == :node
    # opts : Hash
    #   :read_age - how long to read from index after close for writing (read only)
    #   :rotation_age - how long to write to index
    #   :close_age - how long to keep index before closing (default false) after
    #                close for writing (read only)
    #   :client_read_age - how long to keep index before closing (default false)
    #                      after close for writing (read only) for client
    def rotation(rotate_type, opts = {})
      return false unless check_opts(rotate_type, opts)
      # rotate read index
      rotate_read_index

      # rotate client read index
      rotate_client_read_index

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
      [:read_age, :rotation_age, :close_age, :client_read_age].each { |key|
        raise ArgumentError.new("Missing rotation opts '#{key}'") unless
          opts.include?(key)
      }
      # compute time
      [:read_age, :rotation_age].each { |key|
        @opts_rotation[key] = Util::parse_date_offset(opts[key])
      }
      [:close_age, :client_read_age].each { |key|
        @opts_rotation[key] = [false, nil, 'false'].include?(opts[key]) ?
          false : Util::parse_date_offset(opts[key])
      }
      true
    end

    def close_index
      return true unless @opts_rotation[:close_age]

      # find index(es) removed from read, with _read_all parameter
      indexes  = get_indexes(
        "#{@idx_opts[:base]}#{@idx_opts["#{@rotate_type}_suffix".to_sym]}" +
        "_#{KEYS[:read_all]}"
      )
      return true unless indexes
      indexes.each { |index_real, aliases|
        # just for sure skip current write index - contains :current sufix
        #   -- just cannot happened !
        p index_real
        next unless aliases['aliases'].select { |s|
          s.match(/#{Regexp.escape(recognize_index(:write))}/)
        }.empty?

        next unless too_old?(
          parse_index_date(aliases, KEYS[:read_from]),
          @opts_rotation[:close_age]
        )

        request_elastic(:post, "/#{index_real}/_close", {})
      }
    end

    # in case of too old index for CLIENT read, remove CLIENT read alias from index
    # args:
    #   index_alias - main alias name of index
    #   read_age    - read age of index
    def rotate_client_read_index
      indexes  = get_indexes(recognize_index(:read, :client))
      return false if !indexes || indexes.count <= 1
      indexes.each { |index_real, aliases|
        # skip current write index - contains :current sufix
        next unless aliases['aliases'].select { |s|
          s.match(/#{Regexp.escape(recognize_index(:write))}/)
        }.empty?

        next unless too_old?(
          parse_index_date(aliases, KEYS[:read_from]),
          @opts_rotation[:client_read_age]
        )

        # remove :read alias (name of alias depends on configuration on current app)
        request_elastic(
          :delete, "/#{index_real}/_alias/#{recognize_index(:read, :client)}", {}
        )
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
          s.match(/#{Regexp.escape(recognize_index(:write))}/)
        }.empty?

        next unless too_old?(
          parse_index_date(aliases, KEYS[:read_from]),
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
      date_index = Util::now.strftime('%Y_%m_%d')

      # new name based on configuration on server
      new_base_prefix = "#{@idx_opts[:base]}#{@idx_opts[:"#{@rotate_type}_suffix"]}"
      # new index real name
      new_base_index  = "#{new_base_prefix}_#{Util::now.strftime('%Y%m%d%H%M')}"

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

      # add client read information
      request_elastic(:put,
        "#{new_base_index}/_alias/#{recognize_index(:read, :client)}", {}
      )

      # add information about read only to old index (date)
      request_elastic(:put, "#{index_real}/_alias/" +
        "#{new_base_prefix}_#{KEYS[:read_from]}_#{date_index}", {}
      )

    end

    def recognize_index(access, type = :system)
      return @idx if @rotate_type == :direct
      value = eval "@idx_#{@rotate_type}_#{access.to_s}" if type == :system
      value = eval "@idx_#{@rotate_type}_read_client"    if type == :client
      return nil unless value
      value
    end

    # get index with current alias
    def get_indexes(index_alias)
      result = request_elastic :get, "#{index_alias}/_alias/*"
      return false unless result # already logged
      if result['error']
        return nil
      end
      result
    end

    # get date from index alias
    def parse_index_date(key, suffix)
      date = key['aliases'].select { |a| a.match(/#{suffix}_(.+)/) }
      if date.count == 0
        return nil
      end
      date = date.keys.first =~ /#{suffix}_(.+)/i ? $1.gsub('_', '-') : nil
    end

    # equal index date AND age date
    def too_old?(date, age)
      return false unless age && date
      DateTime.parse(date).to_time.to_i <= age.to_i ? true : false
    end

  end # Rotation

end
