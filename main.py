# Your name: Kishan Sripada
# Your student id: 5212 0336
# Your email: ksripada@umich.edu
# List who you have worked with on this project:

import unittest
import sqlite3
import json
import os


def read_data(filename):
  full_path = os.path.join(os.path.dirname(__file__), filename)
  f = open(full_path)
  file_data = f.read()
  f.close()
  json_data = json.loads(file_data)
  return json_data


def open_database(db_name):
  path = os.path.dirname(os.path.abspath(__file__))
  conn = sqlite3.connect(path + '/' + db_name)
  cur = conn.cursor()
  return cur, conn


def make_positions_table(data, cur, conn):
  positions = []
  for player in data['squad']:
    position = player['position']
    if position not in positions:
      positions.append(position)
  cur.execute(
    "CREATE TABLE IF NOT EXISTS Positions (id INTEGER PRIMARY KEY, position TEXT UNIQUE)"
  )
  for i in range(len(positions)):
    cur.execute("INSERT OR IGNORE INTO Positions (id, position) VALUES (?,?)",
                (i, positions[i]))
  conn.commit()


def make_players_table(data, cur, conn):
  cur.execute(
    "CREATE TABLE IF NOT EXISTS Players (id INTEGER PRIMARY KEY, name TEXT, position_id INTEGER, birthyear INTEGER, nationality TEXT)"
  )
  for player in data['squad']:
    cur.execute("SELECT id FROM Positions WHERE position=?",
                (player['position'], ))
    position_id = cur.fetchone()[0]
    birthyear = int(player['dateOfBirth'][:4])
    cur.execute(
      "INSERT OR IGNORE INTO Players (id, name, position_id, birthyear, nationality) VALUES (?,?,?,?,?)",
      (player['id'], player['name'], position_id, birthyear,
       player['nationality']))
  conn.commit()


def nationality_search(countries, cur, conn):
  results = []
  for country in countries:
    cur.execute(
      "SELECT name, position_id, nationality FROM Players WHERE nationality=?",
      (country, ))
    results.extend(cur.fetchall())
  return results


def birthyear_nationality_search(age, country, cur, conn):
  birthyear_threshold = 2023 - age
  cur.execute(
    "SELECT name, nationality, birthyear FROM Players WHERE nationality=? AND birthyear<?",
    (country, birthyear_threshold))
  return cur.fetchall()


def position_birth_search(position, age, cur, conn):
  birthyear_threshold = 2023 - age
  cur.execute(
    "SELECT P.name, Pos.position, P.birthyear FROM Players P JOIN Positions Pos ON P.position_id = Pos.id WHERE Pos.position=? AND P.birthyear>?",
    (position, birthyear_threshold))
  return cur.fetchall()


def make_winners_table(data, cur, conn):
  cur.execute(
    "CREATE TABLE IF NOT EXISTS Winners (id INTEGER PRIMARY KEY, name TEXT UNIQUE)"
  )

  for season in data['seasons']:
    if season.get('winner') is not None:
      winner_id = season['winner']['id']
      winner_name = season['winner']['name']
      cur.execute("INSERT OR IGNORE INTO Winners (id, name) VALUES (?,?)",
                  (winner_id, winner_name))
  conn.commit()


def make_seasons_table(data, cur, conn):
  cur.execute(
    "CREATE TABLE IF NOT EXISTS Seasons (id INTEGER PRIMARY KEY, winner_id INTEGER, end_year INTEGER)"
  )

  for season in data['seasons']:
    if season.get('winner') is not None:
      season_id = season['id']
      winner_id = season['winner']['id']
      end_year = int(season['endDate'][:4])
      cur.execute(
        "INSERT OR IGNORE INTO Seasons (id, winner_id, end_year) VALUES (?,?,?)",
        (season_id, winner_id, end_year))
  conn.commit()


def winners_since_search(year, cur, conn):
  year = int(year)
  cur.execute(
    "SELECT Winners.name, COUNT(*) as wins FROM Seasons JOIN Winners ON Seasons.winner_id = Winners.id WHERE Seasons.end_year >= ? GROUP BY Winners.name ORDER BY wins DESC",
    (year, ))
  winners = cur.fetchall()
  return {winner[0]: winner[1] for winner in winners}


class TestAllMethods(unittest.TestCase):

  def setUp(self):
    path = os.path.dirname(os.path.abspath(__file__))
    self.conn = sqlite3.connect(path + '/' + 'Football.db')
    self.cur = self.conn.cursor()
    self.conn2 = sqlite3.connect(path + '/' + 'Football_seasons.db')
    self.cur2 = self.conn2.cursor()

  def test_players_table(self):
    self.cur.execute('SELECT * from Players')
    players_list = self.cur.fetchall()

    self.assertEqual(len(players_list), 30)
    self.assertEqual(len(players_list[0]), 5)
    self.assertIs(type(players_list[0][0]), int)
    self.assertIs(type(players_list[0][1]), str)
    self.assertIs(type(players_list[0][2]), int)
    self.assertIs(type(players_list[0][3]), int)
    self.assertIs(type(players_list[0][4]), str)

  def test_nationality_search(self):
    x = sorted(nationality_search(['England'], self.cur, self.conn))
    self.assertEqual(len(x), 11)
    self.assertEqual(len(x[0]), 3)
    self.assertEqual(x[0][0], "Aaron Wan-Bissaka")

    y = sorted(nationality_search(['Brazil'], self.cur, self.conn))
    self.assertEqual(len(y), 3)
    self.assertEqual(y[2], ('Fred', 2, 'Brazil'))


def main():

  #### FEEL FREE TO USE THIS SPACE TO TEST OUT YOUR FUNCTIONS

  json_data = read_data('football.json')
  cur, conn = open_database('Football.db')
  make_positions_table(json_data, cur, conn)
  make_players_table(json_data, cur, conn)
  conn.close()

  seasons_json_data = read_data('football_PL.json')
  cur2, conn2 = open_database('Football_seasons.db')
  make_winners_table(seasons_json_data, cur2, conn2)
  make_seasons_table(seasons_json_data, cur2, conn2)
  conn2.close()


if __name__ == "__main__":
  main()
  unittest.main(verbosity=2)
