# encoding:utf-8
require 'active_support/core_ext/object/deep_dup'
require 'unicode_utils/downcase'
require 'digest/sha1'

class ElasticsearchRasi
  module Request
    CONTENT_TYPE = { content_type: 'application/json' }

    def request(method, params)
      send_request(method, params)
    end

    def clear_request(method, params)
      response = send_request(method, params)
      return false if response.include?(:error)
      response
    end

  private

    def another_es?(method)
      !@es_another.empty? &&
        !@config[:another_methods].blank? && @config[:another_methods].include?(method)
    end

    def compute_author_hash(author)
      UnicodeUtils.downcase(Digest::SHA1.hexdigest(author))
    end

    def change_type?(config)
      config.include?("#{@rasi_type}_type".to_sym)
    end

    def change_index?(config)
      config.include?("#{@rasi_type}_index".to_sym) ||
        config["#{@rasi_type}_write_date".to_sym]
    end

    def change_index(key, config, params)
      return params[key] if
        params.include?(key) && !change_index?(config)
      if config.include?("#{@rasi_type}_index".to_sym)
        config["#{@rasi_type}_index".to_sym]
      elsif config.include?("#{@rasi_type}_write_date".to_sym)
        "#{config["#{@rasi_type}_write_date_base".to_sym]}_#{Time.now.strftime('%Y%m')}"
      else
        params[key]
      end
    end

    def prepare_params!(config, params)
      return true if @config[:verboom_bulk].blank?
      # params[:type] = config["#{@rasi_type}_type".to_sym] if
      #   params.include?(:type) && change_type?(config)

      # params[:index] = change_index(:index, config, params)

      params[:body].each do |in_data|
        data = in_data.values[0]
        data[:_type] = config["#{@rasi_type}_type".to_sym] if
          data.include?(:_type) && change_type?(config)

        data[:_index] = change_index(:_index, config, data)

        next if data.blank? || data[:data].blank?
        next if data[:data]['author'].blank?

        author =
          if data[:data]['author'].is_a?(Hash)
            data[:data]['author']['name']
          else
            data[:data]['author']
          end

        data[:data]['search_author'] = {
          'name' => author,
          'author_hash' => compute_author_hash(author) }
      end
    end

    def send_request(method, params)
      counter = 0
      begin
        response = @es.send(method, params)
        return response if
          !another_es?(method) || ![:bulk, :index, :update].include?(method.to_sym)
        @es_another.each do |es|
          next if es[:config].include?("save_#{@rasi_type}".to_sym) &&
                  es[:config]["save_#{@rasi_type}".to_sym] == false
          clone_params = params.deep_dup
          prepare_params!(es[:config], clone_params)
          es[:es].send(method.to_sym, clone_params)
        end
        response
      rescue Elasticsearch::Transport::Transport::Errors::InternalServerError => e
        { error: e.message }
      rescue Elasticsearch::Transport::Transport::Errors::BadRequest => e
        { error: e.message }
      rescue Faraday::ConnectionFailed, Faraday::TimeoutError => e
        counter += 1
        return { error: e.message } if counter > ES[:connect_attempts]
        sleep(ES[:connect_sleep])
        retry
      end
    end
  end
end
