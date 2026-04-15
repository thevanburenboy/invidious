require "./base.cr"

#
# This module contains functions related to the "channels" table.
#
module Invidious::Database::Channels
  extend self

  # -------------------
  #  Insert / delete
  # -------------------

  def insert(channel : InvidiousChannel, update_on_conflict : Bool = false)
    channel_array = channel.to_a

    request = <<-SQL
      INSERT INTO channels
      VALUES (#{arg_array(channel_array)})
    SQL

    if update_on_conflict
      request += <<-SQL
        ON CONFLICT (id) DO UPDATE
        SET author = $2, updated = $3
      SQL
    end

    PG_DB.exec(request, args: channel_array)
  end

  # -------------------
  #  Update
  # -------------------

  def update_author(id : String, author : String)
    request = <<-SQL
      UPDATE channels
      SET updated = now(), author = $1, deleted = false
      WHERE id = $2
    SQL

    PG_DB.exec(request, author, id)
  end

  def update_subscription_time(id : String)
    request = <<-SQL
      UPDATE channels
      SET subscribed = now()
      WHERE id = $1
    SQL

    PG_DB.exec(request, id)
  end

  def update_mark_deleted(id : String)
    request = <<-SQL
      UPDATE channels
      SET updated = now(), deleted = true
      WHERE id = $1
    SQL

    PG_DB.exec(request, id)
  end

  # -------------------
  #  Select
  # -------------------

  def select(id : String) : InvidiousChannel?
    request = <<-SQL
      SELECT * FROM channels
      WHERE id = $1
    SQL

    return PG_DB.query_one?(request, id, as: InvidiousChannel)
  end

  def select(ids : Array(String)) : Array(InvidiousChannel)?
    return [] of InvidiousChannel if ids.empty?

    request = <<-SQL
      SELECT * FROM channels
      WHERE id = ANY($1)
    SQL

    return PG_DB.query_all(request, ids, as: InvidiousChannel)
  end
end

#
# This module contains functions related to the "channel_videos" table.
#
module Invidious::Database::ChannelVideos
  extend self

  # -------------------
  #  Insert
  # -------------------

  # This function returns the status of the query (i.e: success?)
  def insert(video : ChannelVideo, with_premiere_timestamp : Bool = false) : Bool
    if with_premiere_timestamp
      last_items = "premiere_timestamp = $9, views = $10"
    else
      last_items = "views = $10"
    end

    request = <<-SQL
      INSERT INTO channel_videos
      VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
      ON CONFLICT (id) DO UPDATE
      SET title = $2, published = $3, updated = $4, ucid = $5,
          author = $6, length_seconds = $7, live_now = $8, #{last_items}
      RETURNING (xmax=0) AS was_insert
    SQL

    return PG_DB.query_one(request, *video.to_tuple, as: Bool)
  end

  # -------------------
  #  Select
  # -------------------

  def select(ids : Array(String)) : Array(ChannelVideo)
    return [] of ChannelVideo if ids.empty?

    request = <<-SQL
      SELECT * FROM channel_videos
      WHERE id = ANY($1)
      ORDER BY published DESC
    SQL

    return PG_DB.query_all(request, ids, as: ChannelVideo)
  end

  def select_notfications(ucid : String, since : Time) : Array(ChannelVideo)
    request = <<-SQL
      SELECT * FROM channel_videos
      WHERE ucid = $1 AND published > $2
      ORDER BY published DESC
      LIMIT 15
    SQL

    return PG_DB.query_all(request, ucid, since, as: ChannelVideo)
  end

  def select_popular_videos : Array(ChannelVideo)
    decay = (ENV["POPULAR_DECAY"]? || "0.00002").to_f
    power = (ENV["POPULAR_POWER"]? || "0.8").to_f
    view_offset = (ENV["POPULAR_VIEW_OFFSET"]? || "0").to_i
    limit = (ENV["POPULAR_LIMIT"]? || "60").to_i
    community_power = (ENV["POPULAR_COMMUNITY_POWER"]? || "2").to_f
    community_prior = (ENV["POPULAR_COMMUNITY_PRIOR"]? || "5").to_f

    request = <<-SQL
      WITH user_stats AS (
        SELECT COUNT(*) AS total_users FROM users
      ),
      subscription_expanded AS (
        SELECT unnest(subscriptions) AS ucid
        FROM users
      ),
      channel_stats AS (
        SELECT COUNT(DISTINCT ucid)::float AS total_channels
        FROM subscription_expanded
      ),
      subscription_stats AS (
        SELECT COUNT(*)::float AS total_subs
        FROM subscription_expanded
      ),
      community AS (
        SELECT
          (subscription_stats.total_subs /
          (user_stats.total_users * channel_stats.total_channels)
          ) AS base_rate
        FROM user_stats, channel_stats, subscription_stats
      ),
      channel_affinity AS (
        SELECT 
          ucid,
          COUNT(*)::float AS sub_count
        FROM subscription_expanded
        GROUP BY ucid
      )

      SELECT v.*
      FROM channel_videos v
      JOIN user_stats us ON TRUE
      JOIN community c ON TRUE
      LEFT JOIN channel_affinity ca ON ca.ucid = v.ucid
      WHERE v.ucid IN (
        SELECT DISTINCT UNNEST(subscriptions) FROM users
      )
      ORDER BY (
        (
          POW(LOG(GREATEST(v.views + #{view_offset}, 1)), #{power}) *
          EXP(-#{decay} * GREATEST(EXTRACT(EPOCH FROM (NOW() - v.published)) / 60, 0))
        )
        *
        POW(
          1 + (
            (
              COALESCE(ca.sub_count, 0) + #{community_prior} * c.base_rate
            ) /
            (us.total_users + #{community_prior})
          ),
          #{community_power}
        )
      ) DESC
      LIMIT #{limit}
    SQL

    PG_DB.query_all(request, as: ChannelVideo)
  end
end
