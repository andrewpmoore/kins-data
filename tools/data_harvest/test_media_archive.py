import json
import tempfile
import unittest
from pathlib import Path
from unittest import mock
from tools.data_harvest import media_archive as media


class AnnualMediaTests(unittest.TestCase):
    def test_barb_exact_year_and_four_screen_audience(self):
        page = b'''<table id="top_programmes"><thead><tr><th>Rank</th><th>Programme</th><th>Date</th><th>Channel</th><th>TV Set (millions)</th><th>Four-screen total (millions)</th></tr></thead><tbody><tr><td>1</td><td>New Series: Series 1, Episode 9</td><td>06 Nov 2097</td><td>BBC</td><td>14.30</td><td>14.94</td></tr></tbody></table>'''
        with mock.patch.object(media.harvest, 'fetch', return_value=page):
            chart = media.barb_television(2097, 'now')
            self.assertEqual(chart['items'][0]['audience'], '14.94m four-screen')
            self.assertEqual(chart['source']['kind'], 'annualTelevisionAudience')
            with self.assertRaises(media.harvest.HarvestError):
                media.barb_television(2098, 'now')

    def test_tv_artwork_requires_one_exact_uk_series(self):
        def show(name, country='GB'):
            return {'show': {'name': name, 'network': {'country': {'code': country}}, 'url': 'https://www.tvmaze.com/shows/1/test', 'image': {'medium': 'https://static.tvmaze.com/cover.jpg'}}}
        with mock.patch.object(media.harvest, 'fetch', return_value=json.dumps([show('New Series')]).encode()):
            self.assertEqual(media.television_artwork('New Series: Series 1, Episode 9')['artworkProviderName'], 'TVmaze')
            self.assertEqual(media.television_artwork('World Cup Final'), {})
        for results in [[show('New Series', 'US')], [show('New Series'), show('New Series')]]:
            with mock.patch.object(media.harvest, 'fetch', return_value=json.dumps(results).encode()):
                self.assertEqual(media.television_artwork('New Series'), {})

    def test_game_cover_matches_the_official_winner_including_apostrophes(self):
        chart = {'source': {'url': 'https://www.interactive.org/awards/result', 'provider': 'AIAS'}, 'items': [{'title': "Next Year's Game"}]}
        page = b'''<img alt="Unrelated game" src="/other.jpg"><img alt="Next Year's Game" src="/images/new.jpg">'''
        with mock.patch.object(media.harvest, 'fetch', return_value=page):
            self.assertEqual(media.game_artwork(chart)['artworkURL'], 'https://www.interactive.org/images/new.jpg')

    def test_new_year_is_published_and_survives_upstream_failure(self):
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder)
            seed = root / 'seed.json'; seed.write_text('{}')
            def chart(kind, year, title, territory):
                return {'id': kind, 'title': kind, 'year': year, 'source': media.harvest.source('Provider', 'https://provider.example/chart', territory, 'Verified annual result', 'now', kind), 'items': [{'rank': 1, 'title': title}]}
            game = chart('awardWinner', 2098, 'Future Game', 'WORLD')
            tv = chart('annualTelevisionAudience', 2097, 'Future TV', 'GB')
            tv['items'][0].update(channel='BBC', audience='12.5m', broadcastDate='25 Dec 2097')
            media.harvest.atomic_json(root / 'v1/latest/gb.json', {'charts': {'games': game}})
            art = {'artworkURL': 'https://image.example/game.jpg', 'artworkSourceURL': 'https://provider.example/game', 'artworkProviderName': 'Provider'}
            with mock.patch.object(media, 'barb_television', return_value=tv), mock.patch.object(media, 'television_artwork', return_value=art), mock.patch.object(media, 'game_artwork', return_value=art):
                media.update_archive(root, seed, ['GB', 'US'], '2098-03-01')
            gb = json.loads((root / 'v1/media/2097/gb.json').read_text())
            self.assertEqual(gb['charts']['television']['items'][0]['artworkURL'], art['artworkURL'])
            us = json.loads((root / 'v1/media/2098/us.json').read_text())
            self.assertNotIn('television', us['charts'])
            self.assertEqual(us['charts']['games']['items'][0]['title'], 'Future Game')
            with mock.patch.object(media, 'barb_television', side_effect=media.harvest.HarvestError('offline')):
                media.update_archive(root, seed, ['GB', 'US'], '2098-03-02')
            self.assertEqual(json.loads((root / 'v1/media/2097/gb.json').read_text())['charts'], gb['charts'])
            # A carried 2098 winner stays in 2098 when the daily snapshot reaches 2099.
            with mock.patch.object(media, 'barb_television', side_effect=media.harvest.HarvestError('not yet published')):
                media.update_archive(root, seed, ['GB', 'US'], '2099-01-01')
            self.assertFalse((root / 'v1/media/2099/us.json').exists())


if __name__ == '__main__':
    unittest.main()
