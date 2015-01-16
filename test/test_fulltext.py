
import os
import sys
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

if sys.version_info < (2,7):
    import unittest2 as unittest
else:
    import unittest
    
from adsdata import extractors

from adsdata import utils

config_file = os.path.join(utils.base_dir(), 'test/adsdata.cfg.test')
config = utils.load_config(config_file)
# config = {}
# config["FULLTEXT_EXTRACT_PATH"] = 'test/data/'
#print "Config file:", config_file

#config = utils.load_config(config_file=config_file)



class FulltextTestCase(unittest.TestCase):
    
    def test_extractor_factory(self):
        print config.keys()
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
        
        
if __name__ == '__main__':
    unittest.main()