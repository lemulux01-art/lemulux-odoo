"""Regression tests without importing FastAPI or starting production workers."""
import ast
import logging
from pathlib import Path
import unittest
from typing import Any

source = Path(__file__).resolve().parents[1] / 'main.py'
nodes = ast.parse(source.read_text()).body
names = {'fl_validar_respuesta_pdf', 'adjuntar_fl_registrado'}
namespace = {'Any': Any, 'logger': logging.getLogger('test')}
exec(compile(ast.Module(body=[n for n in nodes if isinstance(n, ast.FunctionDef)
                            and n.name in names], type_ignores=[]), str(source), 'exec'), namespace)
validate = namespace['fl_validar_respuesta_pdf']


def error(message, title='Invalid Request', status=400):
    return {'ErrorResponse': {'Head': {'ErrorCode': 'E004'}, 'Body': {'errors': [
        {'status': status, 'message': message, 'title': {'en': title}}
    ]}}}


class InvoiceResponseTests(unittest.TestCase):
    def test_accepts_documented_success(self):
        data = {'SuccessResponse': {'Body': {'Invoice': {'message': 'PDF uploaded successfully'}}}}
        for status in (200, 201):
            self.assertEqual(validate(status, data, 'FL-test'), data)

    def test_e004_is_not_automatically_duplicate(self):
        for title in (
            'OrderItem(s) not found 123', 'Invalid seller order item status for document 123',
            'Max limit reached. Total number of invoice documents should be less than or equal to total items in an order',
            'Order contains FBF items. PDF upload only supports FBS orders.',
            'Invoice date must be less than or equal to current date',
        ):
            with self.subTest(title=title), self.assertRaises(Exception):
                validate(400, error('INVALID_REQUEST', title), 'FL-test')

    def test_arbitrary_conflicts_and_already_words_are_errors(self):
        for status, data in [(409, {}), (400, error('already invalid')),
                             (500, error('INVOICE_ALREADY_EXISTS')),
                             (200, error('INVALID_REQUEST'))]:
            with self.subTest(status=status, data=data), self.assertRaises(Exception):
                validate(status, data, 'FL-test')

    def test_explicit_duplicate_only(self):
        for status in (400, 409):
            self.assertTrue(validate(status, error('INVOICE_ALREADY_EXISTS', status=status), 'FL-test')['ya_cargado'])
        mixed = error('INVOICE_ALREADY_EXISTS')
        mixed['ErrorResponse']['Body']['errors'].append({'message': 'ITEM_NOT_FOUND'})
        with self.assertRaises(Exception):
            validate(409, mixed, 'FL-test')

    def test_unexpected_success_payloads_do_not_become_ok(self):
        for data in ({}, [], None, 'ok', {'SuccessResponse': {}},
                     {'SuccessResponse': {'Body': {'Invoice': {'message': 'queued'}}}}):
            with self.subTest(data=data), self.assertRaises(Exception):
                validate(200, data, 'FL-test')

    def test_rejection_is_recorded_for_retry(self):
        records = []
        def upload(*args):
            return validate(400, error('ITEM_NOT_FOUND'), 'FL-test')
        namespace['_fl_pdf_registrar'] = lambda *args, **kwargs: records.append((args, kwargs))
        namespace['adjuntar_comprobante_fl'] = upload
        with self.assertRaises(Exception):
            namespace['adjuntar_fl_registrado']('FL-test', 42)
        self.assertEqual([r[0][1] for r in records], ['pendiente', 'error'])
        self.assertIn('ITEM_NOT_FOUND', records[1][0][2])


if __name__ == '__main__':
    unittest.main()
