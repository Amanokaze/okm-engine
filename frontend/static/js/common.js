const api_host = "http://localhost:8000/"
const interval = 10000
let api_data_ids = {
    "nodeid": 0,
    "podid": 0
}
let charts = {}

let api_data_types = {
    "overall": "table",
    "nodesysco": "table",
    "pod": "table",
    "podts": "chart",
    "container": "table",
    "podnet": "table",
    "podvol": "table"
}


window.onload = function() {
    init_datatables()
    init_charts('podcpu')
    init_charts('podmemory')

    get_api('overall')
    get_api('nodesysco')
    get_api('pod')
    get_api('container')
    get_api('podnet')
    get_api('podvol')
    get_api('podts')

    setTimeout(function run() {
        get_api('overall')
        get_api('nodesysco', api_data_ids.nodeid)
        get_api('pod', api_data_ids.nodeid)
        get_api('container', api_data_ids.podid)
        get_api('podnet', api_data_ids.podid)
        get_api('podvol', api_data_ids.podid)
        get_api('podts', api_data_ids.nodeid)
        setTimeout(run, interval)
    }, interval)

};

const get_api = (api_data_type, api_data_id=0) => {
    if (api_data_id || api_data_type === "overall") {
        id_url = api_data_id === 0 ? "" : "/"+api_data_id 
        api_url = api_host + 'data/' + api_data_type + id_url    

        fetch(api_url)
        .then(response => response.json())
        .then(data => {
            selector = "#" + api_data_type

            if (data.length > 0) {
                if (api_data_types[api_data_type] === "table") {
                    $(selector).dataTable().fnClearTable()
                    $(selector).dataTable().fnAddData(data)
                    
                    if (api_data_type === "overall") {
                        current_ts = data[0]._ontunetime
                        current_time = new Date(current_ts * 1000)
                        
                        $('#current_time').text(current_time.toISOString().replace('T', ' ').substring(0, 19))
                    }
                } else if (api_data_types[api_data_type] === "chart") {
                    if (api_data_type === "podts") {
                        update_charts('podcpu', data, '_cpuusage')
                        update_charts('podmemory', data, '_memoryused')
                    }
                }
            }
        })
    }
}

const init_datatables = () => {
    $("#overall").DataTable({
        data: [],
        columns: [
            {data: '_nodeid'},
            {data: '_nodename'},
            {data: '_cpuusage'},
            {data: '_memoryused'},
            {data: '_swapused'},
            {data: '_memorysize'},
            {data: '_swapsize'},
            {data: '_netusage'},
            {data: '_fsusage'},
            {data: '_fssize'},
            {data: '_fsiusage'},
            {data: '_imgfsusage'},
            {data: '_proccount'}
        ],
        fnDrawCallback: function() {
            $("#overall tbody tr").click(function() {
                let position = $("#overall").dataTable().fnGetPosition(this)
                let nodeid = $("#overall").dataTable().fnGetData(position)._nodeid
                $('.current_node').text($("#overall").dataTable().fnGetData(position)._nodename)
                api_data_ids.nodeid = nodeid
                get_api('nodesysco', api_data_ids.nodeid)
                get_api('pod', api_data_ids.nodeid)
                get_api('podts', api_data_ids.nodeid)
            })
        }
    })

    $("#nodesysco").DataTable({
        data: [],
        columns: [
            {data: '_containername'},
            {data: '_cpuusage'},
            {data: '_memoryused'},
            {data: '_swapused'},
            {data: '_memorysize'},
            {data: '_swapsize'}
        ]
    })   
    
    $("#pod").DataTable({
        data: [],
        columns: [
            {data: '_podid'},
            {data: '_podname'},
            {data: '_cpuusage'},
            {data: '_memoryused'},
            {data: '_swapused'},
            {data: '_memorysize'},
            {data: '_swapsize'},
            {data: '_netusage'},
            {data: '_netrxrate'},
            {data: '_nettxrate'},
            {data: '_netrxerrors'},
            {data: '_nettxerrors'},
            {data: '_volused'},
            {data: '_voliused'},
            {data: '_epstused'},
            {data: '_epstiused'},
            {data: '_proccount'}
        ],
        fnDrawCallback: function() {
            $("#pod tbody tr").click(function() {
                let position = $("#pod").dataTable().fnGetPosition(this)
                let podid = $("#pod").dataTable().fnGetData(position)._podid
                $('.current_pod').text($("#pod").dataTable().fnGetData(position)._podname)
                api_data_ids.podid = podid
                get_api('container', api_data_ids.podid)
                get_api('podnet', api_data_ids.podid)
                get_api('podvol', api_data_ids.podid)
            })
        }
    })
    
    $("#container").DataTable({
        data: [],
        columns: [
            {data: '_containerid'},
            {data: '_containername'},
            {data: '_cpuusage'},
            {data: '_memoryused'},
            {data: '_swapused'},
            {data: '_memorysize'},
            {data: '_swapsize'},
            {data: '_rootfsused'},
            {data: '_rootfsiused'},
            {data: '_logfsused'},
            {data: '_logfsiused'}
        ]
    })

    $("#podnet").DataTable({
        data: [],
        columns: [
            {data: '_devicename'},
            {data: '_netusage'},
            {data: '_netrxrate'},
            {data: '_nettxrate'},
            {data: '_netrxerrors'},
            {data: '_nettxerrors'}
        ]
    })

    $("#podvol").DataTable({
        data: [],
        columns: [
            {data: '_devicename'},
            {data: '_volused'},
            {data: '_voliused'}
        ]
    })    
}

const init_charts = (chart_id) => {
    const ctx = document.getElementById(chart_id).getContext('2d');
    charts[chart_id] = new Chart(ctx, {
        type: 'line',
        data: {
            labels: [],
            datasets: []
        },
        options: {}
    })
}

const update_charts = (chart_id, data, key_column) => {
    let labels = []
    let values = {}
    data.forEach((row) => {
        podname = row._podname
        agenttime = row._agenttime

        if (!(podname in values)) values[podname] = []
        if (!(labels.includes(agenttime))) labels.push(agenttime)

        values[podname].push(row[key_column])
    })

    charts[chart_id].data.labels.pop();
    charts[chart_id].data.datasets.forEach((dataset) => {
        dataset.data.pop();
    });

    charts[chart_id].data.labels = labels.map((el) => new Date(el * 1000))
    charts[chart_id].data.datasets = Object.keys(values).map((el) => {
        return {
            label: el,
            data: values[el]
        }
    })    
    charts[chart_id].options = {
        scales: {
            x: {
                type: 'timeseries'
            }
        }
    }
    charts[chart_id].update();
}