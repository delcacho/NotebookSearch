import './App.css';
import React, { Component } from 'react';
import { 
	ReactiveBase, CategorySearch, SingleRange, ResultCard, ReactiveList, MultiList, SelectedFilters, DateRange
} from '@appbaseio/reactivesearch';
import './style.css';
import logo from './logosmall.png';
import pythonimg from './python.png';
import scalaimg from './scala.png';
import sqlimg from './sql.png';
import rimg from './r.png';

const { ResultCardWrapper } = ReactiveList;

function getImage(lan) {
   switch(lan) {
     case "Python": return pythonimg
     case "Scala": return scalaimg
     case "SQL": return sqlimg
     case "R": return rimg
   }
}

class App extends Component {

    state = {
        value: '',
    };
    onChange = (value) => {
        this.setState({
            value,
        });
    };
    onValueSelected={
    function(value, category, cause, source) {
      console.log("current value and category: ", value, category)
      this.setState({
            value
      })
    }}


    render() {
  return (
    <div className="App"><title>Notebook Search</title>
<h1><img src={logo} alt="Databricks logo" /> go/notebooksearch</h1>
<div className="info"><strong>Note:</strong> Advanced search operators are supported, for example:
 <pre>"scrambled eggs" +(bacon | fries) -steak</pre></div>
<ReactiveBase
				app="notebookIndex"
 url="http://localhost:9200">
<div className="filters-container">

				<CategorySearch
						onChange={(value, triggerQuery, event) => {
						    this.setState({ value }, () => triggerQuery());
						}}
						searchOperators={true}
						componentId="search"
						dataField={["title","body","author"]}
						categoryField="tags"
						placeholder="Search for notebooks"
						value={this.state.value}
						showFilter={true}
						parseSuggestion={(suggestion) => ({
    						label: (
        						<div>
            						{suggestion.source.title} by
            						<span style={{ color: 'dodgerblue', marginLeft: 5 }}>
                					{suggestion.source.author}
            						</span>
        						</div>
    )})}
					/>
<SelectedFilters showClearAll={true} clearAllLabel="Clear filters"/>
<MultiList componentId="language" dataField="language" title="Programming Language" showSearch={false}/>
<MultiList componentId="vertical" dataField="vertical" title="Industry Vertical" showSearch={false}/>
<MultiList componentId="step" dataField="step" title="Stage" showSearch={false}/>
<DateRange componentId="lastRun" dataField="lastRun" title="Date last run"/>
<MultiList componentId="tag" dataField="tags" title="Tags" queryFormat="and"/>
</div>
<div className="result-list-container">
<ReactiveList
    componentId="SearchResult"
    paginationAt="bottom"
    pagination={true}
    dataField="title"
    size={10}
    style={{align:"left"}}
    react={{
        "and": ["search","tag","language","vertical","step","lastRun"]
    }}
    renderItem={(res) => <div><img src={getImage(res.language)}/> <a href={res.url}>{res.title}</a> by <span style={{ color: 'darkgreen', marginLeft: 5 }}>{res.author} <span className="tagbox" style={{ color: 'gray', marginLeft: 5 }}>{res.language}</span> {res.tags.map(function(d, idx){
         return (<span className="tagbox" style={{ color: 'gray', marginLeft: 5 }} key={idx}>{d}</span>)
       })}</span></div>}
/></div>
				</ReactiveBase>
    </div>
  );}
}

export default App;
