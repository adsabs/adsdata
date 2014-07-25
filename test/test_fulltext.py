
import os
import sys
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

if sys.version_info < (2,7):
    import unittest2 as unittest
else:
    import unittest
    
from adsdata import fulltext, exceptions
    
class FulltextTestCase(unittest.TestCase):
    
    def test_extractor_factory(self):
        test_input = [
            ('2000xxx..999..1234L', 'http://foo/bar/baz', 'Foo', fulltext.HttpExtractor),
            ('2000xxx..999..1234L', '/foo/bar/baz.pdf', 'Foo', fulltext.PdfExtractor),
            ('2000xxx..999..1234L', '/foo/bar/baz.xml', 'Foo', fulltext.XMLExtractor),
            ('2000xxx..999..1234L', '/foo/bar/baz.xml', 'Elsevier', fulltext.ElsevierExtractor),
            ('2000xxx..999..1234L', '/foo/bar/baz.ocr', 'Foo', fulltext.PlainTextExtractor),
            ('2000xxx..999..1234L', '/foo/bar/baz.txt', 'Foo', fulltext.PlainTextExtractor),
            ('2000xxx..999..1234L', '/foo/bar.html,/foo/baz.html', 'Foo', fulltext.HtmlExtractor)
            ]
        for bib, path, prov, cls in test_input:
            ext = fulltext.Extractor.factory(bib, path, prov)
            self.assertTrue(isinstance(ext, cls))
            
        self.assertRaises(
            exceptions.UnknownSourceTypeException, 
            fulltext.Extractor.factory,
            '2000xxx..999..1234L', '/foo/bar/baz.doc', 'Foo'
            )
        
    def test_extrator_const(self):
        ext = fulltext.XMLExtractor('2000xxx..999..1234L', '/foo/bar.xml', 'Foo')
        self.assertTrue(ext.extract_dir.endswith('/20/00/xx/x,/,9/99/,,/12/34/L/'))
        
        