const PREC = {
  OR: 1,
  AND: 2,
  COMPARE: 3,
  IN: 4,
  ADD: 5,
  MUL: 6,
  UNARY: 7,
  CALL: 8,
};

function sep1(rule, sep) {
  return seq(rule, repeat(seq(sep, rule)));
}

function sep(rule, sep) {
  return optional(sep1(rule, sep));
}

module.exports = grammar({
  name: 'dharma',

  extras: $ => [/[ \t\r]+/, $.comment],

  word: $ => $.identifier,

  rules: {
    source_file: $ => repeat($._top_item),

    _top_item: $ => choice(
      $._newline,
      $.package_decl,
      $.external_block,
      $.aggregate_def,
      $.flow_def,
      $.action_def,
      $.reactor_def,
      $.view_def,
      $.query_pipeline,
    ),

    package_decl: $ => seq('package', $.path, optional($._newline)),

    external_block: $ => seq(
      'external',
      $._newline,
      repeat1($.external_entry),
    ),

    external_entry: $ => seq(
      field('key', $.identifier),
      ':',
      field('value', $.list_literal),
      optional($._newline),
    ),

    aggregate_def: $ => seq(
      'aggregate',
      field('name', $.identifier),
      optional(seq('extends', $.path)),
      $._newline,
      field('state', $.state_block),
      optional(field('invariant', $.invariant_block)),
    ),

    state_block: $ => seq(
      'state',
      $._newline,
      repeat1($.field_def),
    ),

    field_def: $ => seq(
      optional(choice('public', 'private')),
      field('name', $.identifier),
      ':',
      field('type', $.type_spec),
      optional(seq('=', $.expr)),
      optional($._newline),
    ),

    invariant_block: $ => seq(
      'invariant',
      $._newline,
      repeat1(seq($.expr, optional($._newline))),
    ),

    flow_def: $ => seq(
      'flow',
      field('name', $.identifier),
      $._newline,
      repeat1($.flow_line),
    ),

    flow_line: $ => seq(
      $.enum_literal,
      repeat1(seq('->', $.flow_step)),
      optional($._newline),
    ),

    flow_step: $ => choice(
      $.enum_literal,
      $.flow_action,
    ),

    flow_action: $ => seq('[', $.identifier, ']'),

    action_def: $ => seq(
      'action',
      field('name', $.identifier),
      field('params', $.param_list),
      $._newline,
      optional($.validate_block),
      optional($.apply_block),
    ),

    param_list: $ => seq('(', sep($.param, ','), ')'),

    param: $ => seq(
      field('name', $.identifier),
      ':',
      field('type', $.type_spec),
      optional(seq('=', $.expr)),
    ),

    validate_block: $ => seq(
      'validate',
      $._newline,
      repeat1(seq($.expr, optional($._newline))),
    ),

    apply_block: $ => seq(
      'apply',
      $._newline,
      repeat1(seq($.apply_stmt, optional($._newline))),
    ),

    apply_stmt: $ => choice(
      $.assignment,
      $.method_call,
    ),

    assignment: $ => seq(
      $.path,
      '=',
      $.expr,
    ),

    reactor_def: $ => seq(
      'reactor',
      field('name', $.identifier),
      $._newline,
      repeat1($.reactor_stmt),
    ),

    reactor_stmt: $ => choice(
      $.trigger_stmt,
      $.validate_block,
      $.emit_stmt,
    ),

    trigger_stmt: $ => seq(
      choice('trigger', 'when'),
      $.path,
      optional($._newline),
    ),

    emit_stmt: $ => seq(
      'emit',
      $.call_expr,
      optional($._newline),
    ),

    view_def: $ => seq(
      'view',
      field('name', $.identifier),
      optional($._newline),
      repeat($.view_stmt),
    ),

    view_stmt: $ => seq(choice($.assignment, $.expr), optional($._newline)),

    query_pipeline: $ => seq(
      optional('query'),
      $.query_source,
      repeat(seq('|', $.query_stage)),
      optional($._newline),
    ),

    query_source: $ => choice($.identifier, $.path),

    query_stage: $ => choice(
      $.where_stage,
      $.search_stage,
      $.take_stage,
    ),

    where_stage: $ => seq('where', $.expr),

    search_stage: $ => seq('search', $.search_terms),

    search_terms: $ => sep1(choice($.string, $.identifier), 'or'),

    take_stage: $ => seq('take', $.number),

    expr: $ => choice(
      $.binary_expr,
      $.unary_expr,
      $.call_expr,
      $.method_call,
      $.path,
      $.literal,
      $.list_literal,
      $.map_literal,
      $.parenthesized,
    ),

    binary_expr: $ => choice(
      prec.left(PREC.OR, seq($.expr, choice('or', '||'), $.expr)),
      prec.left(PREC.AND, seq($.expr, choice('and', '&&'), $.expr)),
      prec.left(PREC.COMPARE, seq($.expr, choice('==', '!=', '>=', '<=', '>', '<'), $.expr)),
      prec.left(PREC.IN, seq($.expr, 'in', $.expr)),
      prec.left(PREC.ADD, seq($.expr, choice('+', '-'), $.expr)),
      prec.left(PREC.MUL, seq($.expr, choice('*', '/', '%'), $.expr)),
    ),

    unary_expr: $ => prec(PREC.UNARY, seq(choice('not', '!', '-'), $.expr)),

    call_expr: $ => prec(PREC.CALL, seq(
      field('function', $.identifier),
      field('arguments', $.arg_list),
    )),

    method_call: $ => prec(PREC.CALL, seq(
      field('target', $.path),
      '.',
      field('method', $.identifier),
      field('arguments', $.arg_list),
    )),

    arg_list: $ => seq('(', sep($.expr, ','), ')'),

    parenthesized: $ => seq('(', $.expr, ')'),

    list_literal: $ => seq('[', sep($.expr, ','), ']'),

    map_literal: $ => seq('{', sep($.map_entry, ','), '}'),

    map_entry: $ => seq(
      field('key', choice($.identifier, $.string)),
      ':',
      field('value', $.expr),
    ),

    literal: $ => choice(
      $.number,
      $.string,
      $.enum_literal,
      $.bool,
      $.null,
    ),

    type_spec: $ => seq($.type_core, optional('?')),

    type_core: $ => choice(
      $.enum_type,
      $.list_type,
      $.map_type,
      $.ref_type,
      $.param_type,
      $.primitive_type,
      $.path,
    ),

    enum_type: $ => seq('Enum', '(', sep1($.identifier, ','), ')'),

    list_type: $ => seq('List', '<', $.type_spec, '>'),

    map_type: $ => seq('Map', '<', $.type_spec, ',', $.type_spec, '>'),

    ref_type: $ => seq('Ref', '<', $.path, '>'),

    param_type: $ => seq($.primitive_type, $.type_args),

    type_args: $ => seq('(', sep1($.type_arg, ','), ')'),

    type_arg: $ => seq($.identifier, '=', $.number),

    primitive_type: $ => choice(
      'Int',
      'Bool',
      'Text',
      'Identity',
      'GeoPoint',
      'Decimal',
      'Timestamp',
      'Duration',
      'Currency',
      'Ratio',
      'PubKey',
      'ObjectId',
    ),

    path: $ => seq($.identifier, repeat(seq('.', $.identifier))),

    identifier: $ => /[A-Za-z_][A-Za-z0-9_]*/,

    enum_literal: $ => /'[A-Za-z_][A-Za-z0-9_]*/,

    number: $ => /[0-9]+(\.[0-9]+)?/,

    bool: $ => choice('true', 'false'),

    null: $ => 'null',

    string: $ => token(seq(
      '"',
      repeat(choice(/[^"\\\n]/, /\\./)),
      '"',
    )),

    comment: $ => token(seq('//', /[^\n]*/)),

    _newline: $ => /\n+/,
  }
});
