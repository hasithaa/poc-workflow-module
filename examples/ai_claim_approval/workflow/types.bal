type SubmitReview record {
    string comment;
    string result;
};

// type Claim record {|
//     string claim;
//     string amount;
// |};

type ReviewDetails record {
    string reason;
    string claims;
};

type ApprovalRequest record {
    string reqId;
    string userId;
    boolean approved;
    string comment;
};

type Claim record {|
    string claim;
    string amount;
|};

type ClaimRequest record {|
    string user;
    string id;
    Claim[] claims;
|};
