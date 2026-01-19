use std::collections::BTreeMap;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SourceSpan {
    pub line: usize,
    pub column: usize,
    pub text: String,
}

impl Default for SourceSpan {
    fn default() -> Self {
        Self {
            line: 0,
            column: 0,
            text: String::new(),
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct Spanned<T> {
    pub value: T,
    pub span: SourceSpan,
}

impl<T> Spanned<T> {
    pub fn new(value: T, span: SourceSpan) -> Self {
        Self { value, span }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Header {
    pub namespace: String,
    pub version: String,
    pub imports: Vec<String>,
    pub concurrency: ConcurrencyMode,
}

impl Default for Header {
    fn default() -> Self {
        Self {
            namespace: "default".to_string(),
            version: "0.0.0".to_string(),
            imports: Vec::new(),
            concurrency: ConcurrencyMode::Strict,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ConcurrencyMode {
    Strict,
    Allow,
}

impl ConcurrencyMode {
    pub fn from_str(value: &str) -> Option<Self> {
        match value {
            "strict" => Some(ConcurrencyMode::Strict),
            "allow" => Some(ConcurrencyMode::Allow),
            _ => None,
        }
    }

    pub fn as_str(&self) -> &'static str {
        match self {
            ConcurrencyMode::Strict => "strict",
            ConcurrencyMode::Allow => "allow",
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct AstFile {
    pub header: Header,
    pub package: Option<String>,
    pub external: Option<ExternalDef>,
    pub aggregates: Vec<AggregateDef>,
    pub actions: Vec<ActionDef>,
    pub reactors: Vec<ReactorDef>,
    pub views: Vec<ViewDef>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct AggregateDef {
    pub name: String,
    pub extends: Option<String>,
    pub fields: Vec<FieldDef>,
    pub invariants: Vec<Spanned<Expr>>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct FieldDef {
    pub name: String,
    pub typ: TypeSpec,
    pub default: Option<Literal>,
    pub visibility: Visibility,
}

#[derive(Clone, Debug, PartialEq)]
pub struct ActionDef {
    pub name: String,
    pub args: Vec<ArgDef>,
    pub validates: Vec<Spanned<Expr>>,
    pub applies: Vec<Spanned<Assignment>>,
    pub doc: Option<String>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct ReactorDef {
    pub name: String,
    pub trigger: Option<String>,
    pub scope: Option<String>,
    pub validates: Vec<Spanned<Expr>>,
    pub emits: Vec<EmitDef>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct EmitDef {
    pub action: String,
    pub args: Vec<(String, Spanned<Expr>)>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct ArgDef {
    pub name: String,
    pub typ: TypeSpec,
}

#[derive(Clone, Debug, PartialEq)]
pub enum Expr {
    Literal(Literal),
    Path(Vec<String>),
    BinaryOp(Op, Box<Expr>, Box<Expr>),
    UnaryOp(Op, Box<Expr>),
    Call(String, Vec<Expr>),
}

#[derive(Clone, Debug, PartialEq)]
pub struct Assignment {
    pub target: Vec<String>,
    pub value: Expr,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum TypeSpec {
    Int,
    Decimal(Option<u32>),
    Ratio,
    Duration,
    Timestamp,
    Currency,
    Text(Option<usize>),
    Bool,
    Enum(Vec<String>),
    Identity,
    Ref(String),
    GeoPoint,
    List(Box<TypeSpec>),
    Map(Box<TypeSpec>, Box<TypeSpec>),
    Optional(Box<TypeSpec>),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Visibility {
    Public,
    Private,
}

#[derive(Clone, Debug, PartialEq)]
pub enum Literal {
    Int(i64),
    Bool(bool),
    Text(String),
    Enum(String),
    Null,
    List(Vec<Expr>),
    Map(Vec<(Expr, Expr)>),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Op {
    Add,
    Sub,
    Mul,
    Div,
    Mod,
    In,
    Eq,
    Neq,
    Gt,
    Lt,
    Gte,
    Lte,
    And,
    Or,
    Not,
    Neg,
}

#[derive(Clone, Debug, PartialEq)]
pub struct CqrsSchemaDoc {
    pub actions: BTreeMap<String, String>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct ExternalDef {
    pub roles: Vec<String>,
    pub time: Vec<String>,
    pub datasets: Vec<String>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct ViewDef {
    pub name: String,
    pub body: Vec<String>,
}
