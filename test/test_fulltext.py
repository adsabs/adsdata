
import os
import sys
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

if sys.version_info < (2,7):
    import unittest2 as unittest
else:
    import unittest
    
from adsdata import extractors, utils
base_dir = utils.get_script_path(file_name_space=__file__)
config_file = os.path.join(base_dir, 'adsdata.cfg.test')
config = utils.load_config(config_file)


class FulltextTestCase(unittest.TestCase):
    
    def test_extractor_factory(self):
        test_input = [
            ('2000xxx..999..1234L', 'http://foo/bar/baz', 'Foo', extractors.HttpExtractor),
            ('2000xxx..999..1234L', '/foo/bar/baz.pdf', 'Foo', extractors.PdfExtractor),
            ('2000xxx..999..1234L', '/foo/bar/baz.xml', 'Foo', extractors.XMLExtractor),
            ('2000xxx..999..1234L', '/foo/bar/baz.xml', 'Elsevier', extractors.ElsevierExtractor),
            ('2000xxx..999..1234L', '/foo/bar/baz.ocr', 'Foo', extractors.PlainTextExtractor),
            ('2000xxx..999..1234L', '/foo/bar/baz.txt', 'Foo', extractors.PlainTextExtractor),
            ('2000xxx..999..1234L', '/foo/bar.html,/foo/baz.html', 'Foo', extractors.HtmlExtractor)
            ]
        for bib, path, prov, cls in test_input:
            ext = extractors.Extractor.factory(bib, path, prov, config)
            self.assertTrue(isinstance(ext, cls))
            
        self.assertRaises(
            extractors.UnknownSourceTypeException, 
            extractors.Extractor.factory,
            '2000xxx..999..1234L', '/foo/bar/baz.doc', 'Foo', config
            )
        
    def test_extrator_const(self):
        ext = extractors.XMLExtractor('2000xxx..999..1234L', '/foo/bar.xml', 'Foo', config)
        self.assertTrue(ext.extract_dir.endswith('/20/00/xx/x,/,9/99/,,/12/34/L/'))
        

class TestExtraction(unittest.TestCase):
    
    def test_xml_extractor_dataset(self):
        expected_output = [u'11456', u'11457', u'11458', u'11459']
        xml_file = '{0}{1}'.format(config['FULLTEXT_EXTRACT_TEST'], 'text.xml')

        XMLExtractor = extractors.Extractor.factory('2000xxx.999..1234L', xml_file, 'Foo', config)
        contents = XMLExtractor.get_contents()

        self.assertEqual(contents['dataset'], expected_output)


if __name__ == '__main__':
    unittest.main()