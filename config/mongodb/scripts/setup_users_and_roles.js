adminDB = db.getSiblingDB('admin');
adminDB.createRole({role:"list_no_read",privileges:[{resource:{db:"local",collection:""},actions:["listCollections"]}],roles:[{role:"readWrite",db:"test"}]})

testDB = db.getSiblingDB('test');
testDB.createUser({user:"list_but_cant_read",pwd:"xyz123",roles:[{"role":"list_no_read","db":"admin"}]})

testDB.createRole({role:"weirdaccess",privileges:[{resource:{db:"test",collection:""},actions:["find"]}],roles:[]});
testDB.createUser({user:"cant_read", pwd: "limited1234", roles: [ {role:"weirdaccess",db:"test"}]})
