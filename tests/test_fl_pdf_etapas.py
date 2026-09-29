"""Pruebas de la subida del PDF Falabella por etapas, sin FastAPI ni base de datos."""
import os
import unittest
from unittest import mock

os.environ.setdefault("DATABASE_URL", "postgresql://x:x@localhost:1/x")
import main as m  # noqa: E402


class EtapaDebida(unittest.TestCase):
    def test_no_sube_antes_de_listo_para_despacho(self):
        for st in ("pending", "processing", "", "canceled", "returned"):
            self.assertIsNone(m.fl_pdf_etapa_debida(st, 1, set()))
            self.assertIsNone(m.fl_pdf_etapa_debida(st, 4, set()))

    def test_primera_subida_en_cualquier_etapa_valida(self):
        for st in ("ready_to_ship", "shipped", "delivered"):
            self.assertEqual(m.fl_pdf_etapa_debida(st, 1, set()), st)
            self.assertEqual(m.fl_pdf_etapa_debida(st, 3, set()), st)

    def test_un_producto_se_sube_una_sola_vez(self):
        self.assertIsNone(m.fl_pdf_etapa_debida("delivered", 1, {"ready_to_ship"}))
        self.assertIsNone(m.fl_pdf_etapa_debida("delivered", 1, {"previa"}))

    def test_varios_productos_se_resuben_al_entregar(self):
        self.assertEqual(m.fl_pdf_etapa_debida("delivered", 4, {"ready_to_ship"}), "delivered")
        self.assertEqual(m.fl_pdf_etapa_debida("delivered", 2, {"previa"}), "delivered")
        self.assertIsNone(m.fl_pdf_etapa_debida("shipped", 4, {"ready_to_ship"}))
        self.assertIsNone(m.fl_pdf_etapa_debida("delivered", 4, {"ready_to_ship", "delivered"}))


def orden(oid, estado, items):
    return {"OrderId": oid, "Statuses": [{"Status": estado}], "ItemsCount": str(items)}


class PorEtapas(unittest.TestCase):
    def correr(self, ordenes, ventas, adjuntar="on"):
        subidas, etapas = [], []
        with mock.patch.dict(m.POST_EMIT, {"falabella": {"adjuntar_fl": adjuntar}}), \
             mock.patch.object(m, "get_venta", side_effect=lambda oid: ventas.get(oid)), \
             mock.patch.object(m, "adjuntar_fl_registrado", side_effect=lambda oid, mv: subidas.append(oid)), \
             mock.patch.object(m, "_fl_pdf_marcar_etapa", side_effect=lambda oid, e: etapas.append((oid, e))), \
             mock.patch.object(m.time, "sleep"):
            n = m.fl_pdf_por_etapas(ordenes, set(ventas))
        return n, subidas, etapas

    def venta(self, estado_pdf="", etapas=""):
        return {"estado": "enviado", "move_id": 1, "fl_pdf_estado": estado_pdf, "fl_pdf_etapas": etapas}

    def test_caso_real_multiproducto_entregado_ya_subido_antes(self):
        n, sub, et = self.correr([orden("1", "delivered", 4)], {"FL-1": self.venta("ok")})
        self.assertEqual(sub, ["FL-1"]); self.assertEqual(et, [("FL-1", "delivered")])

    def test_un_producto_ya_subido_no_se_repite(self):
        n, sub, _ = self.correr([orden("1", "delivered", 1)], {"FL-1": self.venta("ok")})
        self.assertEqual(sub, [])

    def test_en_espera_se_sube_al_pasar_a_listo(self):
        _, sub, et = self.correr([orden("1", "ready_to_ship", 3)], {"FL-1": self.venta("espera")})
        self.assertEqual(et, [("FL-1", "ready_to_ship")])
        _, sub, _ = self.correr([orden("1", "pending", 3)], {"FL-1": self.venta("espera")})
        self.assertEqual(sub, [])

    def test_no_pisa_al_reintento_ni_a_lo_no_emitido(self):
        ventas = {"FL-1": self.venta("error"), "FL-2": self.venta("pendiente"),
                  "FL-3": {"estado": "pendiente", "move_id": None}}
        _, sub, _ = self.correr([orden(i, "delivered", 2) for i in "123"], ventas)
        self.assertEqual(sub, [])

    def test_respeta_el_interruptor_y_el_tope(self):
        ventas = {f"FL-{i}": self.venta("ok") for i in range(20)}
        ords = [orden(str(i), "delivered", 2) for i in range(20)]
        self.assertEqual(self.correr(ords, ventas, adjuntar="off")[0], 0)
        self.assertEqual(self.correr(ords, ventas)[0], 10)


class AlEmitir(unittest.TestCase):
    def emitir(self, estado_envio):
        reg, sub = [], []
        cfg = {"falabella": {"pagar": "off", "email": "off", "adjuntar_fl": "on"}}
        with mock.patch.dict(m.POST_EMIT, cfg), \
             mock.patch.object(m, "get_venta", return_value={"estado_envio": estado_envio}), \
             mock.patch.object(m, "_fl_pdf_registrar", side_effect=lambda oid, e, *a, **k: reg.append(e)), \
             mock.patch.object(m, "adjuntar_fl_registrado", side_effect=lambda oid, mv: sub.append(oid)), \
             mock.patch.object(m, "_fl_pdf_marcar_etapa"):
            m.ejecutar_post_emision(1, "falabella", "FL-1")
        return reg, sub

    def test_pendiente_queda_en_espera_sin_subir(self):
        self.assertEqual(self.emitir("pending"), (["espera"], []))

    def test_listo_para_despacho_sube_al_tiro(self):
        self.assertEqual(self.emitir("ready_to_ship"), ([], ["FL-1"]))


if __name__ == "__main__":
    unittest.main()
