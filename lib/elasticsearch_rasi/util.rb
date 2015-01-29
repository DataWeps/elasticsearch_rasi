#encoding:utf-8
require 'cgi'

module Util

  # convert all keys in the hash to string
  def self.hash_keys_to_str h
    new_h = {}
    h.each_pair { |k, v| new_h[k.to_s] = h[k] }
    new_h
  end

  # assemble additional parameter string from options in hash
  # e.g. {'arg1' => 'val1', 'arg2' => 'val2'} => 'arg1=val1&arg2=val2'
  def self.param_str(h, join = '&')
    arr = []
    h.each_pair{|p,arg| arr.push "#{CGI.escape p.to_s}=#{CGI.escape arg.to_s}" }
    arr.join join
  end

end # Util

