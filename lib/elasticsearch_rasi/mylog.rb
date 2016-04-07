require 'logger'

class MyLog
  class << self
    def log(opts = {})
      return @logger if defined?(@logger)
      @logger = Logger.new(opts[:log_file] || $ES[:log_file])
      @logger.level = opts[:log_level] || $ES[:log_level]
      @logger
    end
  end
end
