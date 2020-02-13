# encoding:utf-8
require 'active_support/core_ext/object/deep_dup'
require 'active_support/core_ext/object/blank'

require 'utils/refines/time_index_name'

module ElasticsearchRasi
  module Request
    using TimeIndexName
    CONTENT_TYPE     = { content_type: 'application/json' }.freeze
    CONNECT_ATTEMPTS = 5
    CONNECT_SLEEP    = 0.1

    def request(method, params)
      send_request(method, params)
    end

    def clear_request(method, params, prepare_params = false)
      response = send_request(method, params, prepare_params)
      return false if response.include?(:error)

      response
    end

  private

    def another_es?(method)
      !@es_another.empty? &&
        @config.another_methods.present? &&
        @config.another_methods.include?(method.to_sym)
    end

    def change_type?(config)
      config.include?("#{@rasi_type}_type".to_sym)
    end

    def change_index?(config)
      config.include?("#{@rasi_type}_index".to_sym) ||
        config["#{@rasi_type}_write_date".to_sym] ||
        config["#{@rasi_type}_lang_index".to_sym]
    end

    def change_read_index(config, params)
      Common.prepare_read_index(
        params[:index],
        config.read_date,
        config.read_date_months,
        config.ignore_max_age)
    end

    def change_write_index(config, params)
      Common.prepare_index(
        params[:_index],
        params[:data],
        config.max_age,
        config.write_date,
        config.language_index,
        config.languages_write)
    end

    def prepare_params!(method, config, params)
      return prepare_bulk!(config, params) if method == :bulk
      return if method == :mget

      new_index =
        if %i[index update].include?(method)
          change_index(config, params)
        else
          change_read_index(config, params)
        end

      params = {} if new_index.blank?
      params[:index] = new_index
      params
    end

    def prepare_bulk!(config, params)
      params[:body].each do |in_data|
        method = in_data.keys[0]
        data   = in_data[method]
        data[:_type] = config["#{@rasi_type}_type".to_sym] if
          data.include?(:_type) && change_type?(config)
        data[:_index] = change_write_index(config, data)
        data[:data]   = { doc: data[:data] } if method == :update
      end
      params[:body].delete_if do |data|
        data.values[0][:_index].blank?
      end
      params.delete(:body) if params[:body].blank?
      params
    end

    def send_request(method, params, prepare_params = true)
      method = method.to_sym
      counter = 0
      begin
        clone_params = params.deep_dup
        prepare_params!(method, @config, clone_params) if prepare_params
        response = @es.send(method, clone_params) unless clone_params.blank?
        # Strange behavior, sometimes ES gem returns empty result, but with OK headers
        raise(Faraday::ConnectionFailed, 'Blank response') if response.blank?
        return response if
          !another_es?(method) || !%i[bulk index update].include?(method)

        @es_another.each do |es|
          next if es[:config].include?("save_#{@rasi_type}".to_sym) &&
                  es[:config]["save_#{@rasi_type}".to_sym] == false

          clone_params = params.deep_dup
          prepare_params!(method, es[:config], clone_params) if prepare_params
          es[:es].send(method.to_sym, clone_params) unless clone_params.blank?
          # TODO: recovery from missing documents : change update method to index method
        end
        response
      rescue Elasticsearch::Transport::Transport::Errors::InternalServerError => e
        { 'errors' => e.message }
      rescue Elasticsearch::Transport::Transport::Errors::BadRequest => e
        { 'errors' => e.message }
      rescue Faraday::ConnectionFailed, Faraday::TimeoutError,
             Elasticsearch::Transport::Transport::Errors::ServiceUnavailable => e
        counter += 1
        return { 'errors' => e.message } if counter > (@config.connect_attempts || CONNECT_ATTEMPTS)

        sleep(@config.connect_sleep || CONNECT_SLEEP)
        retry
      end
    end
  end
end
