
// Power Query M - Template to load canonical fact table from a fixed CSV path.
// In Power BI Desktop: Get Data -> Blank Query -> Advanced Editor -> paste this.
let
    // Change this path once, then keep it stable:
    SourcePath = "C:\ESG_PowerBI\drop\fact_esg_core.csv",
    Source = Csv.Document(File.Contents(SourcePath),[Delimiter=",", Columns=13, Encoding=65001, QuoteStyle=QuoteStyle.Csv]),
    PromotedHeaders = Table.PromoteHeaders(Source, [PromoteAllScalars=true]),
    Typed = Table.TransformColumnTypes(PromotedHeaders,{
        {"Company", type text},
        {"Year", Int64.Type},
        {"MetricCode", type text},
        {"Scope", type text},
        {"Breakdown", type text},
        {"ValueConverted", type number},
        {"UnitConverted", type text},
        {"UnitCanonical", type text},
        {"MetricRaw", type text},
        {"ValueRawStr", type text},
        {"UnitRaw", type text},
        {"SourceFile", type text},
        {"SourceSheet", type text}
    })
in
    Typed
