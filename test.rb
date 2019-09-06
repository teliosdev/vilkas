require "httparty"
require "securerandom"
require "pp"

class Vilkas
  include HTTParty
  base_uri "localhost:3000"

  def create_item(body)
    puts "POST /api/items"
    assert_valid_response(self.class.post("/api/items", body: body.to_json, headers: { "Content-Type" => "application/json" }))
  end

  def item(part, id)
    puts "GET /api/items"
    assert_valid_response(self.class.get("/api/items", query: { part: part, id: id }))
  end

  def view(part, id, user, activity)
    puts "GET /api/view"
    assert_valid_response(self.class.get("/api/view", query: { p: part, i: id, u: user, a: activity }))
  end

  def recommend(part:, user:, current:, whitelist: nil, count: 16)
    body = { part: part, user: user, current: current, whitelist: whitelist, count: count}.to_json
    puts "POST /api/recommend"
    assert_valid_response(self.class.post("/api/recommend", body: body, headers: { "Content-Type": "application/json"}))
  end

  def train(part:)
    puts "GET /api/model/#{part}/train"
    assert_valid_response(self.class.post("/api/model/#{part}/train"))
  end

  def get_model(part:)
    puts "GET /api/model/#{part}"
    assert_valid_response(self.class.get("/api/model/#{part}"))
  end

  private def assert_valid_response(response)
    if response.code >= 300
      pp response
      fail
    else
      response
    end
  end
end

vilkas = Vilkas.new
# b = SecureRandom.uuid
b = "f5a9034e-7d21-448c-a37c-141e103e0d97"
vilkas.create_item(id: b, part: "test", views: 10, meta: { "title" => ["Alphabet"], "i" => ["0"] })
vilkas.item("test", b)

map = {}

(0..10).each do |i|
  id = SecureRandom.uuid
  vilkas.create_item(id: id, part: "test", views: 0, meta: { "title" => ["test #{i}"], "i" => ["#{i}"]})
  map[id] = i
  1.times.each { |_| vilkas.view("test", id, "me", nil) }
end

vilkas.view("test", b, "me", nil)
rec = vilkas.recommend(part: "test", user: "me", current: b)
vilkas.view("test", rec["result"]["items"].first.first, "me", rec["result"]["id"])

(0..10).inject(rec) do |rec, _|
  it = rec["result"]["items"].map(&:first)
  d = it.map { |i| [i, map.fetch(i, 0.0)] }.sort_by(&:last)
  current = d[0][0]

  vilkas.view("test", current, "me", rec["result"]["id"])
  pp vilkas.recommend(part: "test", user: "me", current: current)
end

pp vilkas.train(part: "test")
pp vilkas.get_model(part: "test")