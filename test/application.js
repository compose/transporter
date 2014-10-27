var lookupUser = function(post){
    post.user = lookup("mongodb-production", "users", post.user_id);
    emit(post);
};

var lookupOwner = function(post){};

var flattenEmails = function(post){
    // takes records from source
    (post.user.emails + post.owner.emails).forEach(function(e){
        sink("elasticsearch-prod").emit({user_id: user.id, email: e}, "prod", "emails");
    });
};

var funcs = [lookupUser, lookupOwner, flattenEmails];

Transport({name:"mongodb-production", collection: "posts"}, funcs);

