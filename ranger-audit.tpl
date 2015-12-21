<html>
<title>Ranger Audit</title>
<style>
body {
  background:#eee;
}
.top-navbar {
    color: #ffffff;
    background-color: #58b31f;
    font-family: Anonymous Pro;
    font-size: 18;
    padding: 1px 0 1px 10px
}
</style>
<body>
<header id="top-navbar" class="top-navbar">
    <h2>Ranger Audit</h2>
</header>
<table bgcolor="#808080" cellspacing="1" cellpadding="4">
<tr bgcolor="#c0c0c0">
% for name in colnames:
    <th nowrap>&nbsp;{{name}}&nbsp;</th>
% end
</tr>
% for row in rows:
<tr bgcolor="#ffffff">
%     for col in row:
%         if col.color == 0:
    <td>&nbsp;{{col.value}}&nbsp;</td>
%         else:
    <td>&nbsp;<font color="{{col.color}}">{{col.value}}</font>&nbsp;</td>
%         end
%     end
</tr>
% end
</table>
</body>
</html>

