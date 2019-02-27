# encoding:utf-8

class TranslateLangToCountry
  LANG_TO_COUNTRY = {
    'cs'  => 'cze',
    'sk'  => 'svk',
    'en'  => 'eng',
    'hu'  => 'hun',
    'pl'  => 'pol',
    'de'  => 'deu',
    'es'  => 'mex',
    'mk'  => 'mkd',
    'ru'  => 'rus',
    'lv'  => 'lva',
    'lt'  => 'ltu',
    'sr'  => 'srb',
    'bs'  => 'bih',
    'hr'  => 'hrv',
    'fr'  => 'fra',
    'it'  => 'ita' }.freeze

  def self.translate_lang_to_country(lang)
    return nil unless lang
    [LANG_TO_COUNTRY[[lang].flatten[0]]]
  end
end