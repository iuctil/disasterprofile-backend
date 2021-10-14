
import express from 'express'

const app = express()

app.get('/profile/key', (req, res)=>{
	res.send('Hello World')
})

app.listen(3000)
