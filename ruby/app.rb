require 'sinatra/base'
require 'mysql2'
require 'pathname'
require 'digest/sha2'
require 'redis'
require 'json'
require 'net/sftp'
require 'rack/request'
require 'rack-lineprof'

module Isucon4
  class App < Sinatra::Base
    use Rack::Lineprof
    Redis.current = Redis.new(host: '52.193.220.196')

    helpers do
      def config
        @config ||= {
          db: {
            host: '52.192.211.180',
            port: 3306,
            username: 'root',
            password: 'weitarou',
            database: 'isucon',
          },
        }
      end

      def db
        return Thread.current[:isucon_db] if Thread.current[:isucon_db]
        client = Mysql2::Client.new(
          host: config[:db][:host],
          port: config[:db][:port],
          username: config[:db][:username],
          password: config[:db][:password],
          database: config[:db][:database],
          encoding: 'utf8',
          reconnect: true,
        )
        client.query_options.merge!(symbolize_keys: true, database_timezone: :local, application_timezone: :local)
        Thread.current[:isucon_db] = client
        client
      end

      def advertiser_id
        request.env['HTTP_X_ADVERTISER_ID']
      end

      def redis
        Redis.current
      end

      def ad_key(slot, id)
        "isu4:ad:#{slot}-#{id}"
      end

      def asset_key(slot, id)
        "isu4:asset:#{slot}-#{id}"
      end

      def advertiser_key(id)
        "isu4:advertiser:#{id}"
      end

      def slot_key(slot)
        "isu4:slot:#{slot}"
      end

      def next_ad_id
        redis.incr('isu4:ad-next').to_i
      end

      def get_ad(slot, id)
        key = ad_key(slot, id)
        ad = redis.hgetall(key)

        return nil if !ad || ad.empty?
        ad['impressions'] = ad['impressions'].to_i
        ad['asset'] = url("/slots/#{slot}/ads/#{id}/asset")
        ad['counter'] = url("/slots/#{slot}/ads/#{id}/count")
        ad['redirect'] = url("/slots/#{slot}/ads/#{id}/redirect")
        ad['type'] = nil if ad['type'] == ""

        ad
      end

      def get_log(id)
        g = db.prepare('SELECT * FROM logs WHERE advertiser=?').execute(
          id.split('/').last
        ).map do |x|
          x[:gender] = x[:sex] == 0 ? :female : :male
          x[:ad_id] = x[:ad_id].to_s
          x
        end.group_by { |click| click[:ad_id] }
      end
    end

    get '/' do
      'app'
    end

    post '/slots/:slot/ads' do
      unless advertiser_id
        halt 400
      end

      slot = params[:slot]
      asset = params[:asset][:tempfile]

      id = next_ad_id
      key = ad_key(slot, id)

      redis.hmset(
        key,
        'slot', slot,
        'id', id,
        'title', params[:title],
        'type', params[:type] || params[:asset][:type] || 'video/mp4',
        'advertiser', advertiser_id,
        'destination', params[:destination],
        'impressions', 0,
      )
      ip = ['52.193.220.196', '52.192.211.180'][id % 2]
      Net::SFTP.start(ip, 'root', :password => 'weitarou') do |sftp|
        sftp.file.open("/store/#{id}", "w") do |f|
          f.puts asset.read
        end
      end
      redis.rpush(slot_key(slot), id)
      redis.sadd(advertiser_key(advertiser_id), key)

      content_type :json
      get_ad(slot, id).to_json
    end

    get '/me/report' do
      if !advertiser_id || advertiser_id == ""
        halt 401
      end

      content_type :json

      {}.tap do |report|
        redis.smembers(advertiser_key(advertiser_id)).each do |ad_key|
          ad = redis.hgetall(ad_key)
          next unless ad
          ad['impressions'] = ad['impressions'].to_i

          report[ad['id']] = {ad: ad, clicks: 0, impressions: ad['impressions']}
        end

        get_log(advertiser_id).each do |ad_id, clicks|
          next unless report.include?(ad_id)
          report[ad_id][:clicks] = clicks.size
        end
      end.to_json
    end

    get '/me/final_report' do
      if !advertiser_id || advertiser_id == ""
        halt 401
      end

      content_type :json

      {}.tap do |reports|
        redis.smembers(advertiser_key(advertiser_id)).each do |ad_key|
          ad = redis.hgetall(ad_key)
          next unless ad
          ad['impressions'] = ad['impressions'].to_i

          reports[ad['id']] = {ad: ad, clicks: 0, impressions: ad['impressions']}
        end

        logs = get_log(advertiser_id)

        reports.each do |ad_id, report|
          log = logs[ad_id] || []
          report[:clicks] = log.size

          breakdown = report[:breakdown] = {}

          breakdown[:gender] = log.group_by{ |_| _[:gender] }.map{ |k,v| [k,v.size] }.to_h
          breakdown[:agents] = log.group_by{ |_| _[:agent] }.map{ |k,v| [k,v.size] }.to_h
          breakdown[:generations] = log.group_by{ |_| _[:age] ? _[:age].to_i / 10 : :unknown }.map{ |k,v| [k,v.size] }.to_h
        end
      end.to_json
    end

    post '/initialize' do
      redis.keys('isu4:*').each_slice(1000).map do |keys|
        redis.del(*keys)
      end

      db.prepare('DELETE FROM logs').execute

      content_type 'text/plain'
      "OK"
    end
  end
end
