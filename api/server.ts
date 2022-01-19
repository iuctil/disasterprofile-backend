
import express from 'express';
import cors from 'cors';

import fs from 'fs';

interface IProfile {
    id: string
    name: string
    //long: string 
}

//create a catalog of profileKeys
const profileKeys = [] as IProfile[];
async function cacheProfiles() {
    console.log("enumerating profiles");
    fs.readdir(__dirname+"/../profiles", async (err, files)=>{
        if(err) throw err;
        for await (const file of files/*.splice(0, 100)*/) {
            console.log("parsing", __dirname+"/../profiles/"+file);
            const content = fs.readFileSync(__dirname+"/../profiles/"+file, {encoding: "utf8"});
            const profile = JSON.parse(content);
            const id = file.split(".")[0];
            //console.dir(profile);
            //TODO - I probably need to parse each profile and load proper name/zip code etc..
            profileKeys.push({
                id,
                name: profile.city+", "+profile.state+" "+profile.zip,
                //other: profile.stateAbb+" "+profile.fips, //let user search by other fields
            });
        }
        console.log("loaded", profileKeys.length, "profiles");
    })
}

cacheProfiles();

console.log("starting express server..");
const app = express();
app.use(cors())

app.get('/health', (req, res)=>{
    res.json({
        profiles: profileKeys.length,
        status: "ok",
    });
});

app.get('/profile/keys', (req, res)=>{
    const query = req.query.q?.toString().toLowerCase();

    console.log("received /profile/keys request", query);

    if(!query?.length) return res.status(500).send("query too short");

    const filteredKeys = profileKeys.filter(profile=>{
        if(!query) return true;
        if(profile.name.toLowerCase().includes(query)) return true;
        return false;
    });
    res.send(filteredKeys)
});

app.get('/profile/:id', (req, res)=>{
    const filepath = __dirname+"/../profiles/"+req.params.id+".json";
    console.log("looking for", filepath);
    if(!fs.existsSync(filepath)) return res.status(400).send("no such profile");
    const content = fs.readFileSync(filepath, {encoding: "utf8"});
    const profile = JSON.parse(content);
    console.debug(profile);
    res.send(profile);
});

console.log("listening now on port 3000 ..");
app.listen(3000)
