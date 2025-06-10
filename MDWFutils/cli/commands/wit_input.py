import argparse, ast, sys
from MDWFutils.jobs.wit import generate_wit_input

def register(subparsers):
    p = subparsers.add_parser('wit-input', help='Generate WIT input file')
    p.add_argument('--output-file', required=True,
                   help='Where to write DWF.in')
    p.add_argument('--custom', nargs='*', metavar='SEC.KEY=VAL',
                   help='Override default template fields')
    p.set_defaults(func=do_wit_input)

def do_wit_input(args):
    custom = {}
    if args.custom:
        for tok in args.custom:
            seckey, val = tok.split('=',1)
            sec, key = seckey.split('.',1)
            try:
                v = ast.literal_eval(val)
            except Exception:
                v = val
            custom.setdefault(sec, {})[key] = str(v)
    generate_wit_input(args.output_file, custom_changes=custom)
    return 0