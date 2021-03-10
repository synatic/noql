const data=require('./test/films.json')

const results=data.map(c=>{
   c['Replacement Cost']=parseFloat(c['Replacement Cost'])
    c['Length']=parseInt(c['Length'])
    c.id=c._id
    delete c._id
    return c;
})

require('fs').writeFileSync('./films.json',JSON.stringify(results))
