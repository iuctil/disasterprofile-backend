
import express from 'express';
import cors from 'cors';
import fs from 'fs';
import os from 'os';

// @ts-ignore
import { config } from './config';

interface IProfile {
    id: string
    name: string
    //long: string 
}

interface ICounty {
    state: string //New York
    city: string //New York
    county: string //New York County
    fips: string //36061
}

//create a catalog of profileKeys
const profileKeys = [] as IProfile[];
async function cacheProfiles() {
    console.log("enumerating profiles");
    fs.readdir(__dirname+"/../profiles", async (err, files)=>{
        if(err) throw err;

        if(os.hostname() == "dev.ctil.iu.edu") {
            console.debug("only loading first 100 profiles to save time for dev");
            files = files.splice(0, 100); 
        }

        for await (const file of files/*.splice(0, 100)*/) {
            console.log("parsing", __dirname+"/../profiles/"+file);
            const content = fs.readFileSync(__dirname+"/../profiles/"+file, {encoding: "utf8"});
            const profile = JSON.parse(content);
            const id = file.split(".")[0];

            /*
            profile.counties.forEach((county: ICounty)=>{
                profileKeys.push({
                    id,
                    name: county.city+", "+county.state+" "+profile.zip,
                    //other: profile.stateAbb+" "+profile.fips, //let user search by other fields
                });
            });
            */
            const county = profile.counties[0]; //let's just show the first one for multi-county zip
            profileKeys.push({
                id,
                name: county.city+", "+county.state+" "+profile.zip,
                //other: profile.stateAbb+" "+profile.fips, //let user search by other fields
            });

        }
        console.log("loaded", profileKeys.length, "profiles");
    })
}
cacheProfiles();

interface IRow {
    [key: string]: any
}

function parseCSV(csv: string) {
    const data = [] as IRow[];

    const lines = csv.split("\n");
    // @ts-ignore
    const headers = lines.shift().split(",");

    lines.forEach(line=>{
        const row = {} as IRow;
        const cols = line.split(",");
        for(let i = 0;i < cols.length; ++i) {
            if(headers[i]) row[headers[i]] = cols[i];
        }
        data.push(row);
    });
    return data;
}

interface IDeathRates {
    category: string
    deaths: number
    percentage: number
    crudeRate: number
}

interface IAgeGenderDeathRates {
    [key: string]: IDeathRates[] //keyed by <age>.<gender>
}

const ageGenderDeathRates = {} as IAgeGenderDeathRates;
function organizeDeathRates(data: any[], gender: string) {
    data.forEach(rec=>{
        //convert "25-34" into "25, 26, 27..."
        const ageRange = rec["Age Group"].split("-");
        if(ageRange.length != 2) return;
        const startAge = parseInt(ageRange[0]);
        const endAge = parseInt(ageRange[1]);

        //iterate over each age
        for(let age = startAge; age < endAge; ++age) {
            const key = gender+"."+age;
            const rank = parseInt(rec["Rank"]);
            if(!ageGenderDeathRates[key]) ageGenderDeathRates[key] = [];
            ageGenderDeathRates[key][rank-1] = {
                category: rec["Cause Category"],
                deaths: parseInt(rec["Deaths"]),
                percentage: parseInt(rec["Percentage"]),
                crudeRate: parseInt(rec["Crude Rate"]),
            }
        }
    });
}

function cacheCDC() {
    const maleCSV = fs.readFileSync(config.cdcDeathAgeCSVMale, {encoding: "utf8"});
    organizeDeathRates(parseCSV(maleCSV), "male");
    /*
    0|server  |   {
    0|server  |     Rank: '13',
    0|server  |     'Age Group': '25-34',
    0|server  |     'Cause Category': 'Chronic Low. Respiratory Disease',
    0|server  |     Deaths: '235',
    0|server  |     Percentage: '0.5',
    0|server  |     'Crude Rate': '1.0',
    0|server  |     'Applied Filters': '"All Deaths with drilldown to ICD codes'
    0|server  |   },
    */

    const femaleCSV = fs.readFileSync(config.cdcDeathAgeCSVFemale, {encoding: "utf8"});
    organizeDeathRates(parseCSV(femaleCSV), "female");
}
cacheCDC();
console.dir(ageGenderDeathRates);

///////////////////////////////////////////////////////////////////////////////////////////////////

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

    const gender = req.query.gender;
    const age = req.query.age;

    if(!fs.existsSync(filepath)) return res.status(400).send("no such profile");
    const content = fs.readFileSync(filepath, {encoding: "utf8"});
    const profile = JSON.parse(content);

    //insert age.gender specific profile
    profile.ageGenderDeathRates = ageGenderDeathRates[gender+"."+age];
    res.send(profile);
});

console.log("listening now on port 3000 ..");
app.listen(3000)
