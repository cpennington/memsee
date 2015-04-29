

    # Clean up any old state from previous tutorial runs
    %rm -f ./tutorial.dump
    %rm -f ./tutorial.db


    # First, collect some data to work with
    import meliae.scanner
    with open('./tutorial.dump', 'w') as tutorial_data:
        meliae.scanner.dump_all_objects(tutorial_data)


    # Start covering the memsee app
    import coverage
    cov = coverage.coverage(branch=True, include="memsee.py")
    cov.start()


    %load_ext memsee


    # Create a new memsee database to load data into
    %create tutorial.db

    Database created, available via variable 'memsee'


## Read in the meliae dump


    # Read in the meliae dump
    %read ./tutorial.dump

    Reading
    loaded 10000 objects, 23203 refs
    loaded 20000 objects, 50408 refs
    loaded 30000 objects, 73154 refs
    loaded 40000 objects, 108312 refs
    loaded 50000 objects, 132602 refs
    loaded 60000 objects, 161555 refs
    (sqlite3.OperationalError) cannot commit - no transaction is active [SQL: u'COMMIT']
    
    Marking top objects... None
    60865 (60.9K) objects and 164509 (164.5K) references totalling 10852620 (10.9M) bytes (20.7s)



    %open ./tutorial.db

    Database opened, available via variable 'memsee'



    # Find the most-represented object types
    %select count(*), type from obj group by type order by 1 desc


<script type="text/javascript">
if ($("#dg-css").length == 0){
    $("head").append([
        "<link href='/nbextensions/qgridjs/lib/slick.grid.css' rel='stylesheet'>",
        "<link href='/nbextensions/qgridjs/lib/slick-default-theme.css' rel='stylesheet'>",
        "<link href='http://cdnjs.cloudflare.com/ajax/libs/jqueryui/1.10.4/css/jquery-ui.min.css' rel='stylesheet'>",
        "<link id='dg-css' href='/nbextensions/qgridjs/qgrid.css' rel='stylesheet'>"
    ]);
}
</script>
<div class='q-grid-container'>
<div id='182c719f-dfbf-4034-9fa1-2b397bb0f8d0' class='q-grid'></div>
</div>





    # Enable debugging mode, to see what queries Memsee is executing
    %debug

    DEBUG MODE ON



    %%select parent.*
     from obj as parent,
          ref,
          obj as child
    where parent.address = ref.parent
      and child.address = ref.child
      and child.type = 'str'
    limit 10


<script type="text/javascript">
if ($("#dg-css").length == 0){
    $("head").append([
        "<link href='/nbextensions/qgridjs/lib/slick.grid.css' rel='stylesheet'>",
        "<link href='/nbextensions/qgridjs/lib/slick-default-theme.css' rel='stylesheet'>",
        "<link href='http://cdnjs.cloudflare.com/ajax/libs/jqueryui/1.10.4/css/jquery-ui.min.css' rel='stylesheet'>",
        "<link id='dg-css' href='/nbextensions/qgridjs/qgrid.css' rel='stylesheet'>"
    ]);
}
</script>
<div class='q-grid-container'>
<div id='eba3e2ba-7df7-4097-996e-921e46fd1b45' class='q-grid'></div>
</div>





    # Capture the coverage data
    cov.stop()
    cov.save()


    # Generate a coverage report
    cov.html_report()
    
    from IPython.display import IFrame
    IFrame('htmlcov/memsee.html', 750, 1000)





        <iframe
            width="750"
            height="1000"
            src="htmlcov/memsee.html"
            frameborder="0"
            allowfullscreen
        ></iframe>
        




    
