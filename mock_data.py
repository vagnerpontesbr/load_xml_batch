import random
from datetime import datetime, timedelta
from pathlib import Path
from xml.sax.saxutils import escape

CATEGORIES = [
    ("celular", "Plano Pós-Pago 20GB", "linha"),
    ("televisao", "TV HD Empresarial (1 ponto)", "ponto"),
    ("fibra", "Internet Fibra 300 Mbps", "servico"),
    ("voz", "Voz Ilimitada", "linha"),
    ("iot", "Conectividade IoT", "servico"),
]

OPERADORAS = [
    ("Vivo", "00.000.000/0001-01"),
]

CLIENTES = [
    "Empresa Alpha Ltda",
    "Empresa Beta SA",
    "Empresa Gamma Tech",
    "Empresa Delta Telecom",
    "Empresa Nova Telecom",
    "Empresa Orion Sistemas",
    "Empresa Aurora Digital",
    "Empresa Sigma Networks",
]

DEFAULT_XML_COUNT = 30000


def _rand_date(days_back: int = 90):
    dt = datetime.utcnow() - timedelta(days=random.randint(0, days_back))
    return dt


def _calc_tax(value: float, tipo: str, aliquota: float):
    return {"tipo": tipo, "aliquota": aliquota, "baseCalculo": round(value, 2), "valor": round(value * aliquota, 2)}


def _generate_unique_cnpj(i: int) -> str:
    # Generates a pseudo-unique CNPJ for each invoice based on index
    base = 10000000 + (i % 90000000)
    return f"{base:08d}/0001-{(i % 90) + 10:02d}"


def generate_invoice(i: int, vencimento_fixado: datetime):
    operadora = OPERADORAS[0]
    cliente_nome = random.choice(CLIENTES)
    cliente_cnpj = _generate_unique_cnpj(i)
    start = _rand_date(120).replace(day=1)
    end = (start + timedelta(days=30))
    emissao = end + timedelta(days=1)
    vencimento = vencimento_fixado

    itens = []
    total_servicos = 0.0
    impostos_totais = {"ICMS": 0.0, "PIS": 0.0, "COFINS": 0.0}

    for _ in range(random.randint(2, 5)):
        cat, desc, unidade = random.choice(CATEGORIES)
        valor_base = round(random.uniform(40, 200), 2)
        impostos = [
            _calc_tax(valor_base, "ICMS", 0.25),
            _calc_tax(valor_base, "PIS", 0.0165),
            _calc_tax(valor_base, "COFINS", 0.076),
        ]
        subtotal_impostos = round(sum(t["valor"] for t in impostos), 2)
        total = round(valor_base + subtotal_impostos, 2)
        itens.append({
            "categoria": cat,
            "descricao": desc,
            "quantidade": 1,
            "unidade": unidade,
            "valorBase": valor_base,
            "impostos": impostos,
            "subtotalImpostos": subtotal_impostos,
            "total": total,
        })
        total_servicos += valor_base
        for t in impostos:
            impostos_totais[t["tipo"]] += t["valor"]

    total_impostos = round(sum(impostos_totais.values()), 2)
    total_geral = round(total_servicos + total_impostos, 2)

    return {
        "tipo": "fatura_telecom",
        "operadora": {"nome": operadora[0], "cnpj": operadora[1]},
        "cliente": {"razaoSocial": cliente_nome, "cnpj": cliente_cnpj},
        "contrato": {"numero": f"{random.randint(40000000, 49999999)}"},
        "periodoReferencia": {"inicio": start.isoformat() + "Z", "fim": end.isoformat() + "Z"},
        "datas": {"emissao": emissao.isoformat() + "Z", "vencimento": vencimento.isoformat() + "Z"},
        "moeda": "BRL",
        "itens": itens,
        "totais": {
            "valorServicosSemImpostos": round(total_servicos, 2),
            "impostos": {k: round(v, 2) for k, v in impostos_totais.items()},
            "totalImpostos": total_impostos,
            "totalGeral": total_geral,
        },
        "pagamento": {"metodo": "debito_automatico", "status": "em_aberto"},
        "auditoria": {
            "criadoEm": emissao.isoformat() + "Z",
            "atualizadoEm": emissao.isoformat() + "Z",
            "fonte": "mock_generator",
        },
    }


def generate_invoice_xml(invoice: dict) -> str:
    def to_xml_value(value):
        if isinstance(value, dict):
            return "".join([f"<{k}>{to_xml_value(v)}</{k}>" for k, v in value.items()])
        if isinstance(value, list):
            return "".join([f"<item>{to_xml_value(v)}</item>" for v in value])
        return escape(str(value))

    return "<invoice>" + to_xml_value(invoice) + "</invoice>"


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--count",
        type=int,
        default=DEFAULT_XML_COUNT,
        help=f"Number of XML invoices to generate (default: {DEFAULT_XML_COUNT})",
    )
    parser.add_argument("--due-date", type=str, default=None, help="Fixed due date (YYYY-MM-DD)")
    parser.add_argument("--output", type=str, default="mock_invoices", help="Output directory for XML files")
    args = parser.parse_args()

    due_date = None
    if args.due_date:
        due_date = datetime.strptime(args.due_date, "%Y-%m-%d")

    if due_date is None:
        now = datetime.utcnow()
        due_date = datetime(now.year, 2, 1)

    out_dir = Path(args.output)
    out_dir.mkdir(parents=True, exist_ok=True)

    for i in range(args.count):
        invoice = generate_invoice(i, due_date)
        xml = generate_invoice_xml(invoice)
        file_path = out_dir / f"invoice_{i+1:04d}.xml"
        with open(file_path, "w", encoding="utf-8") as f:
            f.write(xml)

    print(f"Generated {args.count} XML files in {out_dir}")
