[StagingDefinition = [Kind = "FastCopy"]]
section Section1;
shared Documents = let
    Source = SharePoint.Files(
        "https://022gc.sharepoint.com/sites/InformationManagement-BusinessIntelligenceandDecisionSupp333", 
        [ApiVersion = 15]
    ),

    
    #"Filtered Rows" = Table.SelectRows(
        Source, 
        each Text.Contains([Folder Path], "/Shared Documents/Data/Sun Life/Claims/CL90 - Claims Listing/")
    ),

    
    #"Sorted Rows" = Table.Sort(#"Filtered Rows", {{"Date created", Order.Descending}}),

    
    #"Kept Top Rows" = Table.FirstN(#"Sorted Rows", 1),

    
    Navigation = #"Kept Top Rows"{0}[Content],

    
    #"Imported CSV" = Csv.Document(
        Navigation, 
        [Delimiter = ",", Columns = 34, QuoteStyle = QuoteStyle.None]
    ),

    
    #"Promoted Headers" = Table.PromoteHeaders(#"Imported CSV", [PromoteAllScalars = true]),

    
    #"Removed Bottom Rows" = Table.RemoveLastN(#"Promoted Headers", 1)

in
    #"Removed Bottom Rows";
