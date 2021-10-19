
import express from 'express';

import fs from 'fs';

interface IProfile {
    id: string
    name: string
}

//create a catalog of profileKeys
const profileKeys = [] as IProfile[];
console.log("enumerating profiles");
fs.readdir(__dirname+"/../profiles", (err, files)=>{
    if(err) throw err;
    files.forEach(file=>{
        //TODO - I probably need to parse each profile and load proper name/zip code etc..
        profileKeys.push({id: file, name: "Bloomington, Indiana "+file});
    });
    console.log("loaded", profileKeys.length, "profiles");
})

const app = express()

app.get('/profile/keys', (req, res)=>{
    const query = req.query.q?.toString();
    console.dir(req.query);

    console.log("reseived /profile/keys request", query);

    const filteredKeys = profileKeys.filter(profile=>{
        if(!query) return true;
        if(profile.name.includes(query)) return true;
        return false;
    });
	res.send(filteredKeys)
})

console.log("listening now on port 3000 ..");
app.listen(3000)
