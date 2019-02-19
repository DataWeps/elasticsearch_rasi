# encoding:utf-8
require 'cgi'

module ElasticsearchRasi
  class Util
    class << self
      # assemble additional parameter string from options in hash
      # e.g. {'arg1' => 'val1', 'arg2' => 'val2'} => 'arg1=val1&arg2=val2'
      def param_str(h, join = '&')
        h.to_a.map { |s| "#{s[0]}=#{s[-1]}" }.join(join)
      end

      def parse_date_offset(offset)
        now_time = now
        if offset.is_a?(Integer) || offset =~ /^\d+(?:h(?:ours?)?)?$/i
          Time.at(now_time.to_time - offset.to_i * 3600)
        elsif offset =~ /^\d+d(?:ays?)?$/i
          Time.at(now_time.to_time - offset.to_i * 86_400)
        elsif offset =~ /^\d+m(?:months?)?$/i
          new_time = now_time << offset.to_i
          # new_time  = DateTime.now << offset.to_i
          Time.at(new_time.to_time)
        elsif offset =~ /^20\d{2}-[01]\d-[0-3]\d$/
          Time.local(*offset.split('-'))
        else # should not happen
          begin
            Time.parse(offset)
          rescue ArgumentError
            raise "Failed to parse time/offset specification '#{offset}'"
          end
        end
      end

      def now
        DateTime.now
      end
    end
  end
end
